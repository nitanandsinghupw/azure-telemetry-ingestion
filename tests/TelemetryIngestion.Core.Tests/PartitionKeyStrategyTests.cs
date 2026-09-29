using TelemetryIngestion.Core.Partitioning;

namespace TelemetryIngestion.Core.Tests;

public class PartitionKeyStrategyTests
{
    [Fact]
    public void Same_event_always_gets_the_same_key()
    {
        var strategy = new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1), shardCount: 8);
        var e = Events.Create("evt-1");

        Assert.Equal(strategy.GetPartitionKey(e), strategy.GetPartitionKey(e));
    }

    [Fact]
    public void Key_contains_device_and_hour_bucket()
    {
        var strategy = new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1));
        var e = Events.Create("evt-1", "device-7", Events.Now.AddMinutes(42));

        Assert.Equal("device-7|202601011200", strategy.GetPartitionKey(e));
    }

    [Fact]
    public void Different_hours_go_to_different_partitions()
    {
        var strategy = new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1));

        string first = strategy.GetPartitionKey(Events.Create("a", at: Events.Now));
        string second = strategy.GetPartitionKey(Events.Create("b", at: Events.Now.AddHours(1)));

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Shards_spread_one_busy_device_across_all_shards()
    {
        var strategy = new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1), shardCount: 8);

        int distinctKeys = Enumerable.Range(0, 1_000)
            .Select(i => strategy.GetPartitionKey(Events.Create($"evt-{i}")))
            .Distinct()
            .Count();

        Assert.Equal(8, distinctKeys);
    }

    [Fact]
    public void Stable_hash_is_fixed_across_runs()
    {
        // Known FNV-1a value: guards against anyone "simplifying" this to string.GetHashCode().
        Assert.Equal(0x811C9DC5u, StableHash.Fnv1a(string.Empty));
        Assert.Equal(0xE40C292Cu, StableHash.Fnv1a("a"));
    }
}
