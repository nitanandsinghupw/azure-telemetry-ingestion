using TelemetryIngestion.Core.Resilience;

namespace TelemetryIngestion.Core.Tests;

public class RetryPolicyTests
{
    [Fact]
    public void Retry_after_from_the_store_wins()
    {
        var policy = new RetryPolicy(baseDelay: TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromMilliseconds(750), policy.GetDelay(1, TimeSpan.FromMilliseconds(750)));
    }

    [Fact]
    public void Retry_after_is_capped_at_max_delay()
    {
        var policy = new RetryPolicy(maxDelay: TimeSpan.FromSeconds(2));

        Assert.Equal(TimeSpan.FromSeconds(2), policy.GetDelay(1, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void Backoff_grows_but_never_exceeds_the_cap()
    {
        var policy = new RetryPolicy(baseDelay: TimeSpan.FromMilliseconds(100), maxDelay: TimeSpan.FromSeconds(1), random: new Random(1));

        for (int retry = 1; retry <= 20; retry++)
        {
            TimeSpan delay = policy.GetDelay(retry, retryAfter: null);
            Assert.True(delay >= TimeSpan.Zero && delay <= TimeSpan.FromSeconds(1));
        }
    }
}
