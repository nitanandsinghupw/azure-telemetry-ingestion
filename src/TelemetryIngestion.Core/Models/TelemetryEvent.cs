namespace TelemetryIngestion.Core.Models;

/// <summary>A single reading sent by a device (the Event Hubs message body).</summary>
public sealed record TelemetryEvent(
    string EventId,
    string DeviceId,
    DateTimeOffset Timestamp,
    string MetricName,
    double Value);
