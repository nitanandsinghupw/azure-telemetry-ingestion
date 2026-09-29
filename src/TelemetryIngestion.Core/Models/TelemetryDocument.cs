namespace TelemetryIngestion.Core.Models;

/// <summary>
/// The document stored in Cosmos DB.
/// Id = EventId, so replaying the same event overwrites the same document (idempotent upsert).
/// Pk = synthetic partition key produced by an <see cref="Partitioning.IPartitionKeyStrategy"/>.
/// </summary>
public sealed record TelemetryDocument(
    string Id,
    string Pk,
    string DeviceId,
    DateTimeOffset Timestamp,
    string MetricName,
    double Value,
    DateTimeOffset IngestedAt);
