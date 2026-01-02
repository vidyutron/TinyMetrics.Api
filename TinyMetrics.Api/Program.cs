using Dapper;
using Npgsql;
using System.Text.Json;
using TinyMetrics.Api;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<CustomIngestQueue>();
builder.Services.AddSingleton<ShardProvider>();
builder.Services.AddHostedService<ShardIngestWorker>();
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");

var app = builder.Build();

app.MapPost("/api/ingest", async (IngestRequest request, CustomIngestQueue queue, IConfiguration config) =>
{
    // STAGE -1
    // Direct, synchronous write to DB

    //using var connection = new NpgsqlConnection(connectionString);
    //const string sql = "INSERT INTO Events (TenantId, EventType, Payload) VALUES (@TenantId, @EventType, @Payload::jsonb)";

    //await connection.ExecuteAsync(sql, new
    //{
    //    request.TenantId,
    //    request.EventType,
    //    Payload = JsonSerializer.Serialize(request.Payload)
    //});

    // STAGE-2
    // Use Enqueu-Dequeue
    //Console.WriteLine($"enqueing Request - ${request.TenantId}");
    //queue.Enqueue(request);

    // STAGE-3 
    // use staging table to dump the incoming data fast
    //using var conn = new NpgsqlConnection(config.GetConnectionString("DefaultConnection"));
    //const string sql = "INSERT INTO Events_Staging (Content) VALUES (@Content)";
    //// We store the whole thing as a string to move fast
    //await conn.ExecuteAsync(sql, new
    //{
    //    Content = JsonSerializer.Serialize(request)
    //});

    // STAGE-4
    // Introduce sharding
    // Ensure the ID is set at the point of entry
    var record = request with { CorrelationId = request.CorrelationId == Guid.Empty ? Guid.NewGuid() : request.CorrelationId };

    using var conn = new NpgsqlConnection(config.GetConnectionString("DefaultConnection"));
    await conn.ExecuteAsync("INSERT INTO Events_Staging (Content) VALUES (@Content)",
        new { Content = JsonSerializer.Serialize(record) });

    return Results.Accepted();
});

app.Run();

public record IngestRequest(string TenantId, string EventType, object Payload, Guid CorrelationId);