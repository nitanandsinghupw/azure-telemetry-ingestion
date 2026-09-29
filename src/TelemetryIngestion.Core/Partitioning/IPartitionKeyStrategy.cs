using TelemetryIngestion.Core.Models;

namespace TelemetryIngestion.Core.Partitioning;

public interface IPartitionKeyStrategy
{
    string GetPartitionKey(TelemetryEvent telemetryEvent);
}

/// <summary>Naive strategy: one logical partition per device. Hot devices create hot partitions.</summary>
public sealed class DevicePartitionKeyStrategy : IPartitionKeyStrategy
{
    public string GetPartitionKey(TelemetryEvent telemetryEvent) => telemetryEvent.DeviceId;
}
