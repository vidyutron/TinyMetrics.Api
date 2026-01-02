using System.Security.Cryptography;
using System.Text;

public class ShardProvider
{
    // The "Ring" maps a hash position to a specific Connection String
    private readonly Dictionary<uint, string> _ring = new();
    private readonly uint[] _keys;

    // Virtual nodes ensure even data distribution
    private const int VirtualNodeCount = 200;

    public ShardProvider(IConfiguration config)
    {
        var shardNames = new[] { "Shard0", "Shard1" };

        foreach (var name in shardNames)
        {
            var connStr = config.GetConnectionString(name)
                ?? throw new InvalidOperationException($"{name} missing.");

            // Add the physical shard multiple times to the ring (Virtual Nodes)
            for (int i = 0; i < VirtualNodeCount; i++)
            {
                // Create a unique identifier for this virtual spot
                string virtualNodeKey = $"{name}-vnode-{i}";
                uint hash = GetStableHash(virtualNodeKey);
                _ring[hash] = connStr;
            }
        }

        // Cache sorted keys for O(log N) lookup
        _keys = _ring.Keys.OrderBy(k => k).ToArray();
    }

    public string GetConnectionString(string tenantId)
    {
        if (!_ring.Any()) throw new Exception("Shard ring is empty.");

        uint tenantHash = GetStableHash(tenantId);

        // Binary search is O(log N) vs O(N) for linear scan
        int index = Array.BinarySearch(_keys, tenantHash);
        
        // If not found, BinarySearch returns bitwise complement of the next larger element's index
        if (index < 0) index = ~index;
        
        // If we've passed the last node, wrap around to the first one (completing the circle)
        if (index >= _keys.Length) index = 0;

        return _ring[_keys[index]];
    }

    private static uint GetStableHash(string input)
    {
        // MD5/SHA256 are stable; string.GetHashCode() is NOT stable across processes
        var hashBytes = MD5.HashData(Encoding.UTF8.GetBytes(input));
        return BitConverter.ToUInt32(hashBytes, 0);
    }
}