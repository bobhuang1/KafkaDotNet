namespace KafkaDotNet.Contracts;

/// <summary>
/// One line item on an order. Immutable on purpose: an event that flows through
/// Kafka must mean the same thing to every consumer that reads it, possibly days
/// later, so nothing here is mutable.
/// </summary>
public sealed record OrderItem(string Sku, int Quantity, decimal UnitPrice)
{
    /// <summary>Quantity times unit price. Never rounded here — money is rounded at the boundary.</summary>
    public decimal LineTotal => Quantity * UnitPrice;
}

/// <summary>
/// Raised by the API the moment an order is accepted. This is the message that
/// travels on <see cref="OrderTopics.Placed"/>.
/// </summary>
public sealed record OrderPlaced(
    Guid OrderId,
    string CustomerId,
    IReadOnlyList<OrderItem> Items,
    decimal TotalAmount,
    DateTimeOffset OccurredAt);

/// <summary>Emitted by the processor once inventory and payment both succeed.</summary>
public sealed record OrderConfirmed(
    Guid OrderId,
    string CustomerId,
    decimal TotalAmount,
    DateTimeOffset ConfirmedAt);

/// <summary>
/// Emitted when the processor gives up (a permanent failure, or too many retries).
/// The order is parked on <see cref="OrderTopics.DeadLetter"/> for a human to look at.
/// </summary>
public sealed record OrderRejected(
    Guid OrderId,
    string CustomerId,
    string Reason,
    DateTimeOffset RejectedAt);
