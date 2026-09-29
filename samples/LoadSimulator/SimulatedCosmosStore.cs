using System.Collections.Concurrent;
using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;
using TelemetryIngestion.Core.Partitioning;

namespace LoadSimulator;

/// <summary>
/// A stand-in for Cosmos DB that behaves like it where it matters for this demo:
/// logical partition keys map onto a fixed number of physical partitions, and each physical
/// partition can only absorb a limited number of writes per time window. Going over the limit
/// returns a 429 with a Retry-After, exactly like Cosmos DB.
/// </summary>
public sealed class SimulatedCosmosStore : ITelemetryStore
{
    private readonly int _physicalPartitions;
    private readonly int _writesPerWindow;
    private readonly TimeSpan _window;
    private readonly Bucket[] _buckets;

    public SimulatedCosmosStore(int physicalPartitions, int writesPerWindow, TimeSpan window)
    {
        _physicalPartitions = physicalPartitions;
        _writesPerWindow = writesPerWindow;
        _window = window;
        _buckets = Enumerable.Range(0, physicalPartitions).Select(_ => new Bucket()).ToArray();
    }

    public ConcurrentDictionary<string, TelemetryDocument> Documents { get; } = new();
    public ConcurrentDictionary<string, byte> LogicalPartitions { get; } = new();
    public int Throttles;

    public int[] WritesPerPhysicalPartition => _buckets.Select(b => b.TotalWrites).ToArray();

    public Task UpsertAsync(TelemetryDocument document, CancellationToken cancellationToken)
    {
        int physical = (int)(StableHash.Fnv1a(document.Pk) % (uint)_physicalPartitions);
        Bucket bucket = _buckets[physical];

        lock (bucket)
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            if (now - bucket.WindowStart >= _window)
            {
                bucket.WindowStart = now;
                bucket.WritesInWindow = 0;
            }

            if (bucket.WritesInWindow >= _writesPerWindow)
            {
                Interlocked.Increment(ref Throttles);
                TimeSpan retryAfter = _window - (now - bucket.WindowStart);
                throw new StoreThrottledException(retryAfter);
            }

            bucket.WritesInWindow++;
            bucket.TotalWrites++;
        }

        Documents[document.Id] = document;         // upsert: same id overwrites
        LogicalPartitions.TryAdd(document.Pk, 0);
        return Task.CompletedTask;
    }

    private sealed class Bucket
    {
        public DateTimeOffset WindowStart = DateTimeOffset.MinValue;
        public int WritesInWindow;
        public int TotalWrites;
    }
}

public sealed class InMemoryDeadLetterSink : IDeadLetterSink
{
    public ConcurrentBag<DeadLetterRecord> Records { get; } = new();

    public Task SendAsync(DeadLetterRecord record, CancellationToken cancellationToken)
    {
        Records.Add(record);
        return Task.CompletedTask;
    }
}
