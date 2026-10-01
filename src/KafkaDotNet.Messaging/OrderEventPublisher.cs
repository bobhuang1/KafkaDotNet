using System.Globalization;
using Confluent.Kafka;
using KafkaDotNet.Contracts;
using Microsoft.Extensions.Logging;

namespace KafkaDotNet.Messaging;

/// <summary>
/// The one place that knows how an <see cref="OrderPlaced"/> becomes a Kafka
/// record: which topic, which key, and which headers. Keeping it here stops the
/// API from growing a second, subtly different way to publish the same event.
/// </summary>
public sealed class OrderEventPublisher
{
    private readonly IProducer<string, OrderPlaced> _producer;
    private readonly ILogger<OrderEventPublisher> _logger;

    public OrderEventPublisher(IProducer<string, OrderPlaced> producer, ILogger<OrderEventPublisher> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <summary>Publish an order to <see cref="OrderTopics.Placed"/> and return the broker's receipt.</summary>
    public async Task<DeliveryResult<string, OrderPlaced>> PublishAsync(
        OrderPlaced order,
        string? traceId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(order);

        var record = new Message<string, OrderPlaced>
        {
            Key = PartitionKeys.ForCustomer(order.CustomerId),
            Value = order,
            Headers = BuildHeaders(order, traceId),
        };

        var receipt = await _producer.ProduceAsync(OrderTopics.Placed, record, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Published OrderPlaced {OrderId} for {CustomerId} to {Topic}[{Partition}] at offset {Offset}",
            order.OrderId, order.CustomerId, receipt.Topic, receipt.Partition.Value, receipt.Offset.Value);

        return receipt;
    }

    /// <summary>Stamp the headers every OrderPlaced record carries.</summary>
    public static Headers BuildHeaders(OrderPlaced order, string? traceId = null)
    {
        var headers = new Headers();
        headers.SetString(EventHeaders.EventId, Guid.NewGuid().ToString("N"));
        headers.SetString(EventHeaders.EventType, nameof(OrderPlaced));
        headers.SetLong(EventHeaders.Attempt, 0);
        headers.SetString(EventHeaders.OccurredAt, order.OccurredAt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
        if (!string.IsNullOrWhiteSpace(traceId))
        {
            headers.SetString(EventHeaders.TraceId, traceId);
        }

        return headers;
    }
}
