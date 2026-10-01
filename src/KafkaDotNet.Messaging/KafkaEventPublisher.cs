using Confluent.Kafka;
using KafkaDotNet.Contracts;
using Microsoft.Extensions.Logging;

namespace KafkaDotNet.Messaging;

/// <summary>
/// Publishes any event type to any topic, with the retry metadata a consumer needs
/// (attempt number, do-not-deliver-before time, failure reason). The processor uses
/// it for confirmations, rejections, retries and dead letters alike.
/// </summary>
/// <typeparam name="TValue">Event type. Must be registered in DI with a producer.</typeparam>
public sealed class KafkaEventPublisher<TValue> : IEventPublisher<TValue> where TValue : class
{
    private readonly IProducer<string, TValue> _producer;
    private readonly ILogger<KafkaEventPublisher<TValue>> _logger;

    public KafkaEventPublisher(IProducer<string, TValue> producer, ILogger<KafkaEventPublisher<TValue>> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <inheritdoc />
    /// <param name="topic">Destination topic. Use the <see cref="OrderTopics"/> constants.</param>
    /// <param name="key">
    /// Partition key. Reuse the original key so a retry stays on the same partition
    /// and therefore behind whatever came before it.
    /// </param>
    /// <param name="value">The event to publish.</param>
    /// <param name="attempt">Delivery attempt to stamp on the record (0 for the first try).</param>
    /// <param name="notBefore">Earliest time the record may be delivered, for retry records.</param>
    /// <param name="failureReason">Why the previous attempt failed, for dead letters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<DeliveryResult<string, TValue>> PublishAsync(
        string topic,
        string key,
        TValue value,
        int attempt = 0,
        DateTimeOffset? notBefore = null,
        string? failureReason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(value);

        var headers = new Headers();
        headers.SetString(EventHeaders.EventId, Guid.NewGuid().ToString("N"));
        headers.SetString(EventHeaders.EventType, typeof(TValue).Name);
        headers.SetLong(EventHeaders.Attempt, attempt);
        if (notBefore is not null)
        {
            headers.SetNotBefore(notBefore.Value);
        }

        if (!string.IsNullOrWhiteSpace(failureReason))
        {
            headers.SetString(EventHeaders.FailureReason, failureReason);
        }

        var record = new Message<string, TValue> { Key = key, Value = value, Headers = headers };
        var receipt = await _producer.ProduceAsync(topic, record, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Produced {EventType} to {Topic}[{Partition}] offset {Offset} (attempt {Attempt})",
            typeof(TValue).Name, receipt.Topic, receipt.Partition.Value, receipt.Offset.Value, attempt);

        return receipt;
    }
}
