using System.Diagnostics;
using LoadSimulator;
using TelemetryIngestion.Core.Models;
using TelemetryIngestion.Core.Partitioning;
using TelemetryIngestion.Core.Pipeline;
using TelemetryIngestion.Core.Resilience;

// Workload: 20 devices, but one "hot" device sends 60% of all telemetry.
const int TotalEvents = 6_000;
const int BatchSize = 500;          // like an Event Hubs trigger batch
const int Devices = 20;
const double HotDeviceShare = 0.6;

// Simulated Cosmos DB: 4 physical partitions, each absorbing 150 writes per 100 ms.
const int PhysicalPartitions = 4;
const int WritesPerWindow = 150;
TimeSpan window = TimeSpan.FromMilliseconds(100);

List<TelemetryEvent> events = GenerateEvents(TotalEvents, Devices, HotDeviceShare, seed: 42);

Console.WriteLine("Azure telemetry ingestion - hot partition simulation");
Console.WriteLine($"{TotalEvents:N0} events, {Devices} devices, one device sends {HotDeviceShare * 100:F0}% of traffic.");
Console.WriteLine($"Store: {PhysicalPartitions} physical partitions x {WritesPerWindow} writes per {window.TotalMilliseconds} ms.");
Console.WriteLine();

var scenarios = new (string Name, IPartitionKeyStrategy Strategy)[]
{
    ("Naive  pk = deviceId", new DevicePartitionKeyStrategy()),
    ("Bucketed pk = deviceId|hour|shard(8)", new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1), shardCount: 8)),
};

Console.WriteLine($"{"Strategy",-38} {"Time",8} {"429s",7} {"Written",8} {"DLQ",5} {"Busiest partition",18}");
Console.WriteLine(new string('-', 90));

foreach (var (name, strategy) in scenarios)
{
    var store = new SimulatedCosmosStore(PhysicalPartitions, WritesPerWindow, window);
    var deadLetters = new InMemoryDeadLetterSink();
    var pipeline = new IngestionPipeline(
        store, deadLetters, strategy,
        new RetryPolicy(maxAttempts: 12, baseDelay: TimeSpan.FromMilliseconds(20), maxDelay: TimeSpan.FromSeconds(1)),
        new ThrottleGate(),
        new IngestionOptions { MaxConcurrency = 32 });

    var sw = Stopwatch.StartNew();
    int written = 0, deadLettered = 0;
    foreach (var batch in events.Chunk(BatchSize))
    {
        IngestionResult result = await pipeline.ProcessAsync(batch, CancellationToken.None);
        written += result.Written;
        deadLettered += result.DeadLettered;
    }
    sw.Stop();

    int[] perPartition = store.WritesPerPhysicalPartition;
    double busiestShare = perPartition.Max() / (double)perPartition.Sum();
    Console.WriteLine($"{name,-38} {sw.Elapsed.TotalSeconds,7:F2}s {store.Throttles,7:N0} {written,8:N0} {deadLettered,5} {busiestShare * 100,16:F0}%");
}

Console.WriteLine();
Console.WriteLine("With an even spread the busiest of 4 partitions would carry 25% of writes.");
Console.WriteLine("Every event is either written or dead-lettered: nothing is lost.");

static List<TelemetryEvent> GenerateEvents(int count, int devices, double hotShare, int seed)
{
    var random = new Random(seed);
    var start = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    var list = new List<TelemetryEvent>(count);
    for (int i = 0; i < count; i++)
    {
        string device = random.NextDouble() < hotShare ? "device-hot" : $"device-{random.Next(1, devices):D2}";
        list.Add(new TelemetryEvent(
            EventId: $"evt-{i:D6}",
            DeviceId: device,
            Timestamp: start.AddMilliseconds(i * 50),
            MetricName: "temperature",
            Value: 20 + random.NextDouble() * 5));
    }
    return list;
}
