using System.Net;
using Microsoft.Azure.Cosmos;
using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;

namespace TelemetryIngestion.Functions;

/// <summary>Writes telemetry documents to Cosmos DB and maps Cosmos errors onto the pipeline's error types.</summary>
public sealed class CosmosTelemetryStore : ITelemetryStore
{
    // Skipping the response body on writes saves bandwidth and latency at high volume.
    private static readonly ItemRequestOptions WriteOptions = new() { EnableContentResponseOnWrite = false };
    private readonly Container _container;

    public CosmosTelemetryStore(Container container) => _container = container;

    public async Task UpsertAsync(TelemetryDocument document, CancellationToken cancellationToken)
    {
        try
        {
            await _container.UpsertItemAsync(document, new PartitionKey(document.Pk), WriteOptions, cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests)
        {
            throw new StoreThrottledException(ex.RetryAfter, ex);
        }
        catch (CosmosException ex) when (ex.StatusCode is HttpStatusCode.ServiceUnavailable
                                                    or HttpStatusCode.RequestTimeout
                                                    or HttpStatusCode.Gone
                                                    or HttpStatusCode.InternalServerError)
        {
            throw new TransientStoreException($"Cosmos DB returned {(int)ex.StatusCode}.", ex);
        }
    }
}

/// <summary>Stores failed events in a separate container, partitioned by day, for inspection and replay.</summary>
public sealed class CosmosDeadLetterSink : IDeadLetterSink
{
    private readonly Container _container;

    public CosmosDeadLetterSink(Container container) => _container = container;

    public Task SendAsync(DeadLetterRecord record, CancellationToken cancellationToken)
    {
        string day = record.FailedAt.UtcDateTime.ToString("yyyyMMdd");
        var document = new
        {
            id = Guid.NewGuid().ToString("N"),
            pk = day,
            eventId = record.EventId,
            deviceId = record.DeviceId,
            reason = record.Reason,
            detail = record.Detail,
            rawPayload = record.RawPayload,
            failedAt = record.FailedAt
        };
        return _container.CreateItemAsync(document, new PartitionKey(day), cancellationToken: cancellationToken);
    }
}
