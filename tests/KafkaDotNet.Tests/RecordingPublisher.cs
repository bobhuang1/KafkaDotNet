using Confluent.Kafka;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

/// <summary>One published record, captured for assertions.</summary>
public sealed record Published<T>(string Topic, string Key, T Value, int Attempt, DateTimeOffset? NotBefore, string? FailureReason);

/// <summary>
/// Records what the routing logic published, so the four branches of
/// <c>OrderPlacedProcessor</c> can be asserted with no broker involved.
/// </summary>
public sealed class RecordingPublisher<T> : IEventPublisher<T> where T : class
{
    public List<Published<T>> Published { get; } = [];

    public Published<T>? Last => Published.Count > 0 ? Published[^1] : null;

    public Task<DeliveryResult<string, T>> PublishAsync(
        string topic,
        string key,
        T value,
        int attempt = 0,
        DateTimeOffset? notBefore = null,
        string? failureReason = null,
        CancellationToken cancellationToken = default)
    {
        Published.Add(new Published<T>(topic, key, value, attempt, notBefore, failureReason));
        return Task.FromResult<DeliveryResult<string, T>>(null!);
    }
}
