namespace KafkaDotNet.Messaging;

/// <summary>
/// Decides how long to wait before a failed message is offered to a consumer
/// again, and when to stop trying altogether.
///
/// Kafka has no built-in delayed delivery, so "wait five seconds, then retry" has
/// to be *our* rule: the producer stamps a <c>not-before</c> time on the retry
/// record and the retry consumer honours it. This type is the arithmetic behind
/// that stamp, kept free of Kafka so it can be unit-tested directly.
/// </summary>
public sealed record RetryPolicy
{
    /// <summary>Total deliveries allowed before the message is dead-lettered. One means "never retry".</summary>
    public int MaxAttempts { get; init; } = 5;

    /// <summary>Delay before the second delivery.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Each delay is this many times the previous one.</summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>Ceiling for the delay, however many attempts have happened.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How long to wait before delivery number <paramref name="attempt"/> (1 = first retry).
    /// Exponential with a hard ceiling.
    /// </summary>
    public TimeSpan DelayFor(int attempt)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);

        var milliseconds = InitialDelay.TotalMilliseconds * Math.Pow(BackoffMultiplier, attempt - 1);
        var capped = Math.Min(milliseconds, MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(capped);
    }

    /// <summary>
    /// Whether another delivery should be scheduled after <paramref name="failureCount"/>
    /// failures. With <see cref="MaxAttempts"/> = 3 a message gets three deliveries in
    /// total: the original and two retries.
    /// </summary>
    public bool ShouldRetry(int failureCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(failureCount, 1);
        return failureCount < MaxAttempts;
    }
}
