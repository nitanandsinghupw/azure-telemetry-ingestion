using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;
using TelemetryIngestion.Core.Partitioning;
using TelemetryIngestion.Core.Pipeline;
using TelemetryIngestion.Core.Resilience;

namespace TelemetryIngestion.Core.Tests;

public class IngestionPipelineTests
{
    private static IngestionPipeline CreatePipeline(ITelemetryStore store, IDeadLetterSink deadLetters, int maxAttempts = 4) =>
        new(store, deadLetters,
            new BucketedPartitionKeyStrategy(TimeSpan.FromHours(1), shardCount: 4),
            new RetryPolicy(maxAttempts),
            new ThrottleGate(),
            new IngestionOptions { MaxConcurrency = 4 },
            delay: (_, _) => Task.CompletedTask,   // no real waiting in tests
            now: () => Events.Now);

    [Fact]
    public async Task Writes_every_valid_event()
    {
        var store = new ScriptedStore((_, _) => null);
        var pipeline = CreatePipeline(store, new RecordingDeadLetters());

        IngestionResult result = await pipeline.ProcessAsync(
            new[] { Events.Create("a"), Events.Create("b"), Events.Create("c") }, CancellationToken.None);

        Assert.Equal(3, result.Written);
        Assert.Equal(3, store.Saved.Count);
        Assert.Equal("a", store.Saved["a"].Id);
    }

    [Fact]
    public async Task Duplicate_event_ids_in_a_batch_are_written_once()
    {
        var store = new ScriptedStore((_, _) => null);
        var pipeline = CreatePipeline(store, new RecordingDeadLetters());

        IngestionResult result = await pipeline.ProcessAsync(
            new[] { Events.Create("a"), Events.Create("a"), Events.Create("b") }, CancellationToken.None);

        Assert.Equal(2, result.Written);
        Assert.Equal(1, result.Duplicates);
        Assert.Equal(1, store.CallsFor("a"));
    }

    [Fact]
    public async Task Throttled_write_is_retried_until_it_succeeds()
    {
        var store = new ScriptedStore((_, call) => call <= 2 ? new StoreThrottledException(TimeSpan.FromMilliseconds(10)) : null);
        var pipeline = CreatePipeline(store, new RecordingDeadLetters());

        IngestionResult result = await pipeline.ProcessAsync(new[] { Events.Create("a") }, CancellationToken.None);

        Assert.Equal(1, result.Written);
        Assert.Equal(2, result.ThrottleRetries);
        Assert.Equal(0, result.DeadLettered);
    }

    [Fact]
    public async Task Event_is_dead_lettered_when_retries_run_out()
    {
        var store = new ScriptedStore((_, _) => new TransientStoreException("service unavailable"));
        var deadLetters = new RecordingDeadLetters();
        var pipeline = CreatePipeline(store, deadLetters, maxAttempts: 3);

        IngestionResult result = await pipeline.ProcessAsync(new[] { Events.Create("a") }, CancellationToken.None);

        Assert.Equal(0, result.Written);
        Assert.Equal(1, result.DeadLettered);
        Assert.Equal(3, store.CallsFor("a"));
        Assert.Equal("RetriesExhausted", Assert.Single(deadLetters.Records).Reason);
    }

    [Fact]
    public async Task Permanent_error_is_dead_lettered_without_retrying()
    {
        var store = new ScriptedStore((_, _) => new InvalidOperationException("document too large"));
        var deadLetters = new RecordingDeadLetters();
        var pipeline = CreatePipeline(store, deadLetters);

        IngestionResult result = await pipeline.ProcessAsync(new[] { Events.Create("a") }, CancellationToken.None);

        Assert.Equal(1, result.DeadLettered);
        Assert.Equal(1, store.CallsFor("a"));
        Assert.Equal("PermanentStoreError", Assert.Single(deadLetters.Records).Reason);
    }

    [Fact]
    public async Task Invalid_event_is_dead_lettered_and_not_written()
    {
        var store = new ScriptedStore((_, _) => null);
        var deadLetters = new RecordingDeadLetters();
        var pipeline = CreatePipeline(store, deadLetters);
        var future = Events.Create("a", at: Events.Now.AddHours(1));

        IngestionResult result = await pipeline.ProcessAsync(new[] { future, Events.Create("b", device: "") }, CancellationToken.None);

        Assert.Equal(0, result.Written);
        Assert.Equal(2, result.DeadLettered);
        Assert.All(deadLetters.Records, r => Assert.Equal("Invalid", r.Reason));
    }

    [Fact]
    public async Task Event_without_timestamp_is_rejected()
    {
        var deadLetters = new RecordingDeadLetters();
        var pipeline = CreatePipeline(new ScriptedStore((_, _) => null), deadLetters);
        var noTimestamp = Events.Create("a") with { Timestamp = default };

        IngestionResult result = await pipeline.ProcessAsync(new[] { noTimestamp }, CancellationToken.None);

        Assert.Equal(1, result.DeadLettered);
        Assert.Equal("Timestamp is missing.", Assert.Single(deadLetters.Records).Detail);
    }

    [Fact]
    public async Task Dead_letter_failure_fails_the_batch_so_it_is_retried()
    {
        var store = new ScriptedStore((_, _) => new InvalidOperationException("permanent"));
        var pipeline = CreatePipeline(store, new RecordingDeadLetters { Fail = true });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ProcessAsync(new[] { Events.Create("a") }, CancellationToken.None));
    }

    [Fact]
    public async Task Counters_add_up_to_the_batch_size()
    {
        var store = new ScriptedStore((doc, _) => doc.Id == "bad" ? new InvalidOperationException("x") : null);
        var pipeline = CreatePipeline(store, new RecordingDeadLetters());
        var batch = new[] { Events.Create("a"), Events.Create("a"), Events.Create("bad"), Events.Create("c", device: "") };

        IngestionResult r = await pipeline.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(r.Received, r.Written + r.DeadLettered + r.Duplicates);
    }
}
