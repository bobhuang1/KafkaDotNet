using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class RetryPolicyTests
{
    private readonly RetryPolicy _policy = new()
    {
        MaxAttempts = 5,
        InitialDelay = TimeSpan.FromSeconds(2),
        BackoffMultiplier = 2.0,
        MaxDelay = TimeSpan.FromSeconds(20),
    };

    [Theory]
    [InlineData(1, 2)]   // first retry waits the initial delay
    [InlineData(2, 4)]   // then doubles
    [InlineData(3, 8)]
    [InlineData(4, 16)]
    public void Delay_doubles_each_attempt(int attempt, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), _policy.DelayFor(attempt));
    }

    [Fact]
    public void Delay_never_exceeds_the_ceiling()
    {
        // 2 * 2^4 would be 32s; the cap is 20s.
        Assert.Equal(TimeSpan.FromSeconds(20), _policy.DelayFor(5));
        Assert.Equal(TimeSpan.FromSeconds(20), _policy.DelayFor(50));
    }

    [Fact]
    public void Delay_is_monotonic_up_to_the_cap()
    {
        for (var attempt = 2; attempt <= 6; attempt++)
        {
            Assert.True(_policy.DelayFor(attempt) >= _policy.DelayFor(attempt - 1));
        }
    }

    [Fact]
    public void Delay_rejects_attempt_zero()
        => Assert.Throws<ArgumentOutOfRangeException>(() => _policy.DelayFor(0));

    [Theory]
    [InlineData(1, true)]   // one failure so far, attempts remain
    [InlineData(4, true)]
    [InlineData(5, false)]  // MaxAttempts reached
    [InlineData(6, false)]
    public void ShouldRetry_stops_at_MaxAttempts(int failureCount, bool expected)
        => Assert.Equal(expected, _policy.ShouldRetry(failureCount));

    [Fact]
    public void A_single_attempt_policy_never_retries()
    {
        var policy = new RetryPolicy { MaxAttempts = 1 };
        Assert.False(policy.ShouldRetry(1));
    }
}
