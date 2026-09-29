namespace TelemetryIngestion.Core.Resilience;

/// <summary>
/// Shared cool-down across all concurrent writers. When one write is throttled, every
/// writer pauses until the cool-down ends, instead of each one keeping up the pressure
/// that caused the 429 in the first place.
/// </summary>
public sealed class ThrottleGate
{
    private readonly Func<DateTimeOffset> _now;
    private long _resumeAtUtcTicks;

    public ThrottleGate(Func<DateTimeOffset>? now = null) => _now = now ?? (() => DateTimeOffset.UtcNow);

    public void Trip(TimeSpan coolDown)
    {
        long candidate = (_now() + coolDown).UtcTicks;
        long current;
        do
        {
            current = Interlocked.Read(ref _resumeAtUtcTicks);
            if (candidate <= current) return; // an equal or longer cool-down is already active
        }
        while (Interlocked.CompareExchange(ref _resumeAtUtcTicks, candidate, current) != current);
    }

    public TimeSpan RemainingCoolDown
    {
        get
        {
            long remaining = Interlocked.Read(ref _resumeAtUtcTicks) - _now().UtcTicks;
            return remaining > 0 ? TimeSpan.FromTicks(remaining) : TimeSpan.Zero;
        }
    }
}
