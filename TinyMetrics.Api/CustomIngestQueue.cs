using System.Collections.Concurrent;

namespace TinyMetrics.Api
{
    public class CustomIngestQueue
    {
        private readonly ConcurrentQueue<IngestRequest> _queue = new();
        private readonly SemaphoreSlim _signal = new(0);

        public void Enqueue(IngestRequest request)
        {
            _queue.Enqueue(request);
            _signal.Release();
        }

        public async Task<IngestRequest> DequeueAsync(CancellationToken ct)
        {
            await _signal.WaitAsync(ct);
            _queue.TryDequeue(out var request);
            return request!;
        }
    }
}
