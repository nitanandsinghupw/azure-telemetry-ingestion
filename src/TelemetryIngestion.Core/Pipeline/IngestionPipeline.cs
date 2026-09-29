using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;
using TelemetryIngestion.Core.Partitioning;
using TelemetryIngestion.Core.Resilience;

namespace TelemetryIngestion.Core.Pipeline;

/// <summary>
/// Processes one batch of telemetry: validate, de-duplicate, then write with bounded
/// concurrency and retries.
///
/// Delivery guarantee: when <see cref="ProcessAsync"/> returns, every event in the batch has
/// either been written or dead-lettered, so the caller can safely checkpoint. If the dead-letter
/// sink itself fails, the exception is rethrown so the whole batch is retried; that is safe
/// because writes are idempotent upserts keyed by EventId.
/// </summary>
public sealed class IngestionPipeline
{
    private readonly ITelemetryStore _store;
    private readonly IDeadLetterSink _deadLetters;
    private readonly IPartitionKeyStrategy _partitionKeys;
    private readonly RetryPolicy _retry;
    private readonly ThrottleGate _gate;
    private readonly IngestionOptions _options;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<DateTimeOffset> _now;

    public IngestionPipeline(
        ITelemetryStore store,
        IDeadLetterSink deadLetters,
        IPartitionKeyStrategy partitionKeys,
        RetryPolicy retry,
        ThrottleGate gate,
        IngestionOptions? options = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<DateTimeOffset>? now = null)
    {
        _store = store;
        _deadLetters = deadLetters;
        _partitionKeys = partitionKeys;
        _retry = retry;
        _gate = gate;
        _options = options ?? new IngestionOptions();
        _delay = delay ?? ((d, ct) => Task.Delay(d, ct));
        _now = now ?? (() => DateTimeOffset.UtcNow);
    }

    public async Task<IngestionResult> ProcessAsync(IReadOnlyList<TelemetryEvent> events, CancellationToken cancellationToken)
    {
        var counters = new Counters();
        var accepted = new List<TelemetryEvent>(events.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        DateTimeOffset now = _now();

        foreach (TelemetryEvent e in events)
        {
            string? problem = Validate(e, now, _options.MaxClockSkew);
            if (problem is not null)
            {
                await DeadLetterAsync(e, "Invalid", problem, counters, cancellationToken);
                continue;
            }

            // Event Hubs is at-least-once, so the same event can appear twice in a batch.
            if (!seen.Add(e.EventId))
            {
                Interlocked.Increment(ref counters.Duplicates);
                continue;
            }
            accepted.Add(e);
        }

        var parallel = new ParallelOptions
        {
            MaxDegreeOfParallelism = _options.MaxConcurrency,
            CancellationToken = cancellationToken
        };
        await Parallel.ForEachAsync(accepted, parallel, (e, ct) => new ValueTask(WriteAsync(e, counters, ct)));

        return new IngestionResult(
            Received: events.Count,
            Written: counters.Written,
            DeadLettered: counters.DeadLettered,
            Duplicates: counters.Duplicates,
            ThrottleRetries: counters.ThrottleRetries,
            TransientRetries: counters.TransientRetries);
    }

    public static string? Validate(TelemetryEvent e, DateTimeOffset now, TimeSpan maxClockSkew)
    {
        if (string.IsNullOrWhiteSpace(e.EventId)) return "EventId is missing.";
        if (string.IsNullOrWhiteSpace(e.DeviceId)) return "DeviceId is missing.";
        if (string.IsNullOrWhiteSpace(e.MetricName)) return "MetricName is missing.";
        if (double.IsNaN(e.Value) || double.IsInfinity(e.Value)) return "Value is not a finite number.";
        if (e.Timestamp == default) return "Timestamp is missing.";
        if (e.Timestamp > now + maxClockSkew) return $"Timestamp {e.Timestamp:O} is too far in the future.";
        return null;
    }

    private async Task WriteAsync(TelemetryEvent e, Counters counters, CancellationToken ct)
    {
        var document = new TelemetryDocument(
            Id: e.EventId,
            Pk: _partitionKeys.GetPartitionKey(e),
            DeviceId: e.DeviceId,
            Timestamp: e.Timestamp,
            MetricName: e.MetricName,
            Value: e.Value,
            IngestedAt: _now());

        for (int attempt = 1; ; attempt++)
        {
            TimeSpan coolDown = _gate.RemainingCoolDown;
            if (coolDown > TimeSpan.Zero)
            {
                // Small random spread so paused writers don't all retry at the same instant.
                TimeSpan jitter = TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * _retry.BaseDelay.TotalMilliseconds);
                await _delay(coolDown + jitter, ct);
            }

            try
            {
                await _store.UpsertAsync(document, ct);
                Interlocked.Increment(ref counters.Written);
                return;
            }
            catch (StoreThrottledException ex) when (attempt < _retry.MaxAttempts)
            {
                Interlocked.Increment(ref counters.ThrottleRetries);
                // Pause every writer, not just this one: the partition is saturated.
                _gate.Trip(_retry.GetDelay(attempt, ex.RetryAfter));
            }
            catch (TransientStoreException) when (attempt < _retry.MaxAttempts)
            {
                Interlocked.Increment(ref counters.TransientRetries);
                await _delay(_retry.GetDelay(attempt, retryAfter: null), ct);
            }
            catch (TransientStoreException ex)
            {
                await DeadLetterAsync(e, "RetriesExhausted", $"{attempt} attempts: {ex.Message}", counters, ct);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await DeadLetterAsync(e, "PermanentStoreError", ex.Message, counters, ct);
                return;
            }
        }
    }

    private async Task DeadLetterAsync(TelemetryEvent e, string reason, string detail, Counters counters, CancellationToken ct)
    {
        var record = new DeadLetterRecord(e.EventId, e.DeviceId, reason, detail, RawPayload: null, FailedAt: _now());
        await _deadLetters.SendAsync(record, ct); // failures propagate: the batch must be retried
        Interlocked.Increment(ref counters.DeadLettered);
    }

    private sealed class Counters
    {
        public int Written;
        public int DeadLettered;
        public int Duplicates;
        public int ThrottleRetries;
        public int TransientRetries;
    }
}
