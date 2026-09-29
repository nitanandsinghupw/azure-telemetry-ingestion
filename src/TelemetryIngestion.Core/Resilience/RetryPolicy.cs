namespace TelemetryIngestion.Core.Resilience;

/// <summary>
/// Exponential backoff with full jitter. If the store says how long to wait
/// (Retry-After on a 429), that value wins, because guessing shorter just causes more 429s.
/// </summary>
public sealed class RetryPolicy
{
    private readonly Random _random;

    public RetryPolicy(int maxAttempts = 6, TimeSpan? baseDelay = null, TimeSpan? maxDelay = null, Random? random = null)
    {
        if (maxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        MaxAttempts = maxAttempts;
        BaseDelay = baseDelay ?? TimeSpan.FromMilliseconds(100);
        MaxDelay = maxDelay ?? TimeSpan.FromSeconds(10);
        _random = random ?? Random.Shared;
    }

    /// <summary>Total tries including the first one.</summary>
    public int MaxAttempts { get; }
    public TimeSpan BaseDelay { get; }
    public TimeSpan MaxDelay { get; }

    /// <param name="retryNumber">1 for the first retry, 2 for the second, and so on.</param>
    /// <param name="retryAfter">Delay requested by the store, if any.</param>
    public TimeSpan GetDelay(int retryNumber, TimeSpan? retryAfter)
    {
        if (retryAfter is { } requested && requested > TimeSpan.Zero)
            return requested <= MaxDelay ? requested : MaxDelay;

        double exponentialMs = BaseDelay.TotalMilliseconds * Math.Pow(2, Math.Max(0, retryNumber - 1));
        double cappedMs = Math.Min(exponentialMs, MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(_random.NextDouble() * cappedMs);
    }
}
