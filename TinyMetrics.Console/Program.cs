using System.Net.Http.Json;

var client = new HttpClient { BaseAddress = new Uri("https://localhost:7121") }; // Match your API port
var tasks = new List<Task>();

Console.WriteLine("Starting load test...");

for (int i = 0; i < 50000; i++)
{
    var tenantId = $"tenant-{i % 5}"; // Simulating 5 different customers
    tasks.Add(client.PostAsJsonAsync("/api/ingest", new
    {
        TenantId = tenantId,
        EventType = "page_view",
        Payload = new { url = "/home", browser = "Chrome" }
    }));
}

await Task.WhenAll(tasks);
Console.WriteLine("Done!");