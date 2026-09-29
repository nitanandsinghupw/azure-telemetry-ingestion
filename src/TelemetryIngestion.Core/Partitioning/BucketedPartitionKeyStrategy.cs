using System.Globalization;
using System.Text;
using TelemetryIngestion.Core.Models;

namespace TelemetryIngestion.Core.Partitioning;

/// <summary>
/// Synthetic partition key: "{deviceId}|{time bucket}|{shard}".
///
/// - The time bucket keeps any single logical partition bounded (Cosmos DB caps a
///   logical partition at 20 GB) and moves writes onto new partitions over time.
/// - The shard spreads a single very busy device across several logical partitions,
///   so its writes land on more than one physical partition instead of throttling one.
///
/// Trade-off: reading one device's data for one bucket needs ShardCount point
/// queries (or one cross-shard query). Choose ShardCount from the hottest device's write rate.
///
/// The shard uses a stable FNV-1a hash, never string.GetHashCode(), which is randomised
/// per process in .NET and would send the same event to different partitions after a restart.
/// </summary>
public sealed class BucketedPartitionKeyStrategy : IPartitionKeyStrategy
{
    private readonly TimeSpan _bucketSize;
    private readonly int _shardCount;

    public BucketedPartitionKeyStrategy(TimeSpan bucketSize, int shardCount = 1)
    {
        if (bucketSize <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(bucketSize));
        if (shardCount < 1) throw new ArgumentOutOfRangeException(nameof(shardCount));
        _bucketSize = bucketSize;
        _shardCount = shardCount;
    }

    public int ShardCount => _shardCount;

    public string GetPartitionKey(TelemetryEvent telemetryEvent)
    {
        long utcTicks = telemetryEvent.Timestamp.UtcTicks;
        long bucketTicks = utcTicks - (utcTicks % _bucketSize.Ticks);
        string bucket = new DateTime(bucketTicks, DateTimeKind.Utc)
            .ToString("yyyyMMddHHmm", CultureInfo.InvariantCulture);

        if (_shardCount == 1) return $"{telemetryEvent.DeviceId}|{bucket}";

        uint shard = StableHash.Fnv1a(telemetryEvent.EventId) % (uint)_shardCount;
        return $"{telemetryEvent.DeviceId}|{bucket}|{shard}";
    }
}

public static class StableHash
{
    /// <summary>32-bit FNV-1a over UTF-8 bytes. Same input gives the same output on every machine and run.</summary>
    public static uint Fnv1a(string value)
    {
        uint hash = 2166136261;
        foreach (byte b in Encoding.UTF8.GetBytes(value))
        {
            hash ^= b;
            hash *= 16777619;
        }
        return hash;
    }
}
