namespace KafkaDotNet.Messaging;

/// <summary>
/// The publishing surface the domain logic depends on. Depending on this instead
/// of <see cref="KafkaEventPublisher{TValue}"/> lets the retry/route decisions be
/// unit-tested with a recording fake, with no broker in sight.
/// </summary>
/// <typeparam name="TValue">Event type being published.</typeparam>
public interface IEventPublisher<TValue> where TValue : class
{
    /// <summary>Produce one record and wait for the broker acknowledgement.</summary>
    Task<Confluent.Kafka.DeliveryResult<string, TValue>> PublishAsync(
        string topic,
        string key,
        TValue value,
        int attempt = 0,
        DateTimeOffset? notBefore = null,
        string? failureReason = null,
        CancellationToken cancellationToken = default);
}
