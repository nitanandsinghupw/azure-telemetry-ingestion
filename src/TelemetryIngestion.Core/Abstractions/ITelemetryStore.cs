using TelemetryIngestion.Core.Models;

namespace TelemetryIngestion.Core.Abstractions;

/// <summary>
/// Storage for telemetry documents (Cosmos DB in production).
/// Implementations must translate provider errors:
/// throttling (HTTP 429) -> <see cref="StoreThrottledException"/>,
/// temporary failures (408, 503, timeouts) -> <see cref="TransientStoreException"/>.
/// Any other exception is treated as permanent and the event is dead-lettered.
/// </summary>
public interface ITelemetryStore
{
    Task UpsertAsync(TelemetryDocument document, CancellationToken cancellationToken);
}

public interface IDeadLetterSink
{
    Task SendAsync(DeadLetterRecord record, CancellationToken cancellationToken);
}

public class TransientStoreException : Exception
{
    public TransientStoreException(string message, Exception? inner = null) : base(message, inner) { }
}

public sealed class StoreThrottledException : TransientStoreException
{
    public StoreThrottledException(TimeSpan? retryAfter, Exception? inner = null)
        : base("The store is throttling requests (HTTP 429).", inner) => RetryAfter = retryAfter;

    /// <summary>How long the store asked us to wait, if it said.</summary>
    public TimeSpan? RetryAfter { get; }
}
