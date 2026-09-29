namespace TelemetryIngestion.Core.Pipeline;

public sealed class IngestionOptions
{
    /// <summary>Maximum parallel writes per batch. This is the backpressure knob for the store.</summary>
    public int MaxConcurrency { get; init; } = 16;

    /// <summary>Events stamped further than this in the future are rejected as invalid.</summary>
    public TimeSpan MaxClockSkew { get; init; } = TimeSpan.FromMinutes(5);
}
