namespace TelemetryIngestion.Core.Models;

/// <summary>Counters for one processed batch. Received = Written + DeadLettered + Duplicates.</summary>
public sealed record IngestionResult(
    int Received,
    int Written,
    int DeadLettered,
    int Duplicates,
    int ThrottleRetries,
    int TransientRetries);
