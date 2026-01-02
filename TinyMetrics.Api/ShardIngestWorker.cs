﻿using Dapper;
using Npgsql;
using System.Text.Json;

namespace TinyMetrics.Api
{
    public class ShardIngestWorker : BackgroundService
    {
        private readonly ShardProvider _shardProvider;
        private readonly ILogger<ShardIngestWorker> _logger;
        private readonly string _stagingConnString;

        public ShardIngestWorker(ShardProvider shardProvider, IConfiguration config, ILogger<ShardIngestWorker> logger)
        {
            _shardProvider = shardProvider;
            _logger = logger;
            _stagingConnString = config.GetConnectionString("DefaultConnection");
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var stagingConn = new NpgsqlConnection(_stagingConnString);

                // 1. EXTRACT: Grab a batch of raw data
                var rawRows = await stagingConn.QueryAsync<(long Id, string Content)>(
                    "SELECT Id, Content FROM Events_Staging WHERE IsProcessed = FALSE LIMIT 500");

                if (!rawRows.Any())
                {
                    await Task.Delay(2000, stoppingToken);
                    continue;
                }

                // 2. TRANSFORM: Deserialize and Group by Shard (with Poison Pill handling)
                var validItems = new List<(long Id, IngestRequest Request)>();
                var failedIds = new List<long>();

                foreach (var row in rawRows)
                {
                    try
                    {
                        var req = JsonSerializer.Deserialize<IngestRequest>(row.Content);
                        if (req is not null) validItems.Add((row.Id, req));
                        else failedIds.Add(row.Id);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to deserialize event {Id}. Marking as processed to unblock.", row.Id);
                        failedIds.Add(row.Id);
                    }
                }

                var groups = validItems.GroupBy(x => _shardProvider.GetConnectionString(x.Request.TenantId));

                // 3. LOAD: Parallel batch inserts with IDEMPOTENCY
                var uploadTasks = groups.Select(async group =>
                {
                    using var shardConn = new NpgsqlConnection(group.Key);

                    // 'ON CONFLICT DO NOTHING' ensures we don't create duplicates during retries
                    const string sql = @"
                    INSERT INTO Events (CorrelationId, TenantId, EventType, Payload) 
                    VALUES (@CorrelationId, @TenantId, @EventType, @Payload::jsonb)
                    ON CONFLICT (CorrelationId) DO NOTHING";

                    await shardConn.ExecuteAsync(sql, group.Select(g => new {
                        g.Request.CorrelationId,
                        g.Request.TenantId,
                        g.Request.EventType,
                        Payload = JsonSerializer.Serialize(g.Request.Payload)
                    }));
                });

                await Task.WhenAll(uploadTasks);

                // 4. CLEANUP: Mark as processed in the staging DB
                // We mark both successfully processed items AND failed items (poison pills)
                var ids = validItems.Select(x => x.Id).Concat(failedIds).ToList();
                
                await stagingConn.ExecuteAsync("UPDATE Events_Staging SET IsProcessed = TRUE WHERE Id = ANY(@ids)", new { ids });
            }
        }
    }
}
