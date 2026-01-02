
using Dapper;
using Npgsql;
using System.Text.Json;

namespace TinyMetrics.Api
{
    public class IngestWorker : BackgroundService
    {
        private readonly CustomIngestQueue _queue;
        private readonly string _connString;

        // Add batching 
        private const int MaxBatchSize = 100;
        private readonly TimeSpan _maxWaitTime = TimeSpan.FromSeconds(5);

        public IngestWorker(CustomIngestQueue queue, IConfiguration config)
        {
            _queue = queue;
            _connString = config.GetConnectionString("DefaultConnection");
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // STAGE - 1, simple one by one write to DB
            //while (!stoppingToken.IsCancellationRequested) { 

            //    var req = await _queue.DequeueAsync(stoppingToken);
            //    try
            //    {
            //        using var conn = new NpgsqlConnection(_connString);
            //        await conn.ExecuteAsync(
            //            "INSERT INTO Events (TenantId, EventType, Payload) VALUES (@TenantId, @EventType, @Payload::jsonb)",
            //            new { req.TenantId, req.EventType, Payload = JsonSerializer.Serialize(req.Payload) }
            //        );
            //    }
            //    catch(Exception ex)
            //    {
            //        Console.WriteLine($"Worker Error: {ex.Message}");
            //    }
            //}

            // STAGE - 2, simple batching

            //var batch = new List<IngestRequest>();
            //while (!stoppingToken.IsCancellationRequested)
            //{
            //    try
            //    {
            //        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            //        cts.CancelAfter(_maxWaitTime);
            //        try
            //        {
            //            var request = await _queue.DequeueAsync(cts.Token);
            //            batch.Add(request);
            //        }
            //        catch (OperationCanceledException ex)
            //        {
            //            // Timeout reached
            //        }

            //        if(batch.Count>0 && (batch.Count>=MaxBatchSize || stoppingToken.IsCancellationRequested || !cts.IsCancellationRequested))
            //        {
            //            await ProcessBatchAsync(batch);
            //            batch.Clear();
            //        }
            //    }
            //    catch (Exception)
            //    {

            //        throw;
            //    }
            //}

            // STAGE-3
            while (!stoppingToken.IsCancellationRequested)
            {
                using var conn = new NpgsqlConnection(_connString);
                await conn.OpenAsync(stoppingToken);

                // 1. Get a batch of raw events
                var rawEvents = await conn.QueryAsync<(long Id, string Content)>(
                    "SELECT Id, Content FROM Events_Staging WHERE IsProcessed = FALSE LIMIT 100");

                if (!rawEvents.Any())
                {
                    await Task.Delay(1000, stoppingToken); // Wait if nothing to do
                    continue;
                }

                // 2. Map to our objects
                var batch = rawEvents.Select(x => JsonSerializer.Deserialize<IngestRequest>(x.Content)).ToList();

                // 3. Insert into the MAIN Events table (Batching)
                await ProcessBatchAsync(batch);

                // 4. Mark as processed (or DELETE for better performance)
                var ids = rawEvents.Select(x => x.Id).ToList();
                await conn.ExecuteAsync("UPDATE Events_STAGING SET IsProcessed = TRUE WHERE Id = ANY(@ids)", new { ids });
            }
        }

        private async Task ProcessBatchAsync(List<IngestRequest> items)
        {
            using var conn = new NpgsqlConnection(_connString);
            const string sql = @"
            INSERT INTO Events (TenantId, EventType, Payload) 
            VALUES (@TenantId, @EventType, @Payload::jsonb)";

            var parameters = items.Select(x => new {
                x.TenantId,
                x.EventType,
                Payload = JsonSerializer.Serialize(x.Payload)
            });

            await conn.ExecuteAsync(sql, parameters);
            Console.WriteLine($"[Worker] Flushed {items.Count} items to Postgres.");
        }
    }
}
