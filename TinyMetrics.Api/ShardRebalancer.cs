using Dapper;
using Npgsql;

namespace TinyMetrics.Api
{
    public class ShardRebalancer
    {
        private readonly ShardProvider _shardProvider;
        private readonly string[] _allShardConnStrings;

        public ShardRebalancer(ShardProvider shardProvider, IConfiguration config)
        {
            _shardProvider = shardProvider;
            _allShardConnStrings = [
                config.GetConnectionString("Shard0"),
                config.GetConnectionString("Shard1"),
                config.GetConnectionString("Shard2")
                ];
        }

        public async Task RunRebalanceAsync()
        {
            foreach(var currShardConn in _allShardConnStrings)
            {
                Console.WriteLine($"Checking Shard: {new NpgsqlConnectionStringBuilder(currShardConn).Database}");
                using var conn = new NpgsqlConnection(currShardConn);
                // 1. Find all unique tenants on this physical shard
                var tenants = await conn.QueryAsync<string>("SELECT DISTINCT TenantId FROM Events");

                foreach (var tenantId in tenants)
                {
                    // 2. Ask the ShardProvider where this tenant SHOULD be
                    var targetShardConn = _shardProvider.GetConnectionString(tenantId);

                    // 3. If the current shard is NOT the target, we must move the data
                    if (targetShardConn != currShardConn)
                    {
                        await MigrateTenantDataAsync(tenantId, currShardConn, targetShardConn);
                    }
                }
            }
        }

        private async Task MigrateTenantDataAsync(string tenantId, string sourceConn, string targetConn)
        {
            Console.WriteLine($"[MOVE] Tenant {tenantId} is in the wrong place. Moving to new shard...");

            using var source = new NpgsqlConnection(sourceConn);
            using var target = new NpgsqlConnection(targetConn);

            // 4. Use a Transaction to ensure we don't lose data during the move
            // Note: Distributed transactions are hard, so we use a "Copy then Delete" approach
            var data = await source.QueryAsync("SELECT * FROM Events WHERE TenantId = @tenantId", new { tenantId });

            // Insert into the new home (Idempotency handles duplicates)
            const string insertSql = @"
            INSERT INTO Events (CorrelationId, TenantId, EventType, Payload) 
            VALUES (@CorrelationId, @TenantId, @EventType, @Payload::jsonb)
            ON CONFLICT (CorrelationId) DO NOTHING";

            await target.ExecuteAsync(insertSql, data);

            // 5. Delete from the old home only AFTER successful insert
            await source.ExecuteAsync("DELETE FROM Events WHERE TenantId = @tenantId", new { tenantId });

            Console.WriteLine($"[SUCCESS] Tenant {tenantId} migration complete.");
        }
    }
}
