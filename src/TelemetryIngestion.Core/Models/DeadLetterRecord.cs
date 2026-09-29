namespace TelemetryIngestion.Core.Models;

/// <summary>An event that could not be stored, with the reason. Nothing is silently dropped.</summary>
public sealed record DeadLetterRecord(
    string? EventId,
    string? DeviceId,
    string Reason,
    string Detail,
    string? RawPayload,
    DateTimeOffset FailedAt);
