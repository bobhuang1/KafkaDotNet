using KafkaDotNet.Contracts;

namespace KafkaDotNet.OrderProcessor.Orders;

/// <summary>
/// Stands in for "reserve inventory, authorise payment". It is deterministic so
/// the demo always shows the same three paths: a normal success, a large order
/// that fails once and then succeeds, and an order for a SKU that is permanently
/// out of stock.
/// </summary>
public sealed class SimulatedOrderHandler(ILogger<SimulatedOrderHandler> logger) : IOrderHandler
{
    /// <summary>Any order containing this SKU fails permanently — the demo's dead-end case.</summary>
    public const string UnavailableSku = "OUT-OF-STOCK";

    /// <summary>Orders above this total fail the first attempt (the demo's retry case).</summary>
    public const decimal LargeOrderThreshold = 5_000m;

    private static readonly TimeSpan SimulatedWork = TimeSpan.FromMilliseconds(50);

    /// <inheritdoc />
    public async Task<HandlingResult> HandleAsync(OrderPlaced order, int attempt, CancellationToken cancellationToken)
    {
        // Pretend to talk to inventory and the payment provider.
        await Task.Delay(SimulatedWork, cancellationToken).ConfigureAwait(false);

        var unavailable = order.Items.FirstOrDefault(
            item => string.Equals(item.Sku, UnavailableSku, StringComparison.OrdinalIgnoreCase));

        if (unavailable is not null)
        {
            return HandlingResult.PermanentFailure($"SKU {unavailable.Sku} is out of stock");
        }

        if (order.TotalAmount > LargeOrderThreshold && attempt == 0)
        {
            return HandlingResult.Transient(
                $"payment provider timed out authorising {order.TotalAmount.ToString("C", System.Globalization.CultureInfo.InvariantCulture)}");
        }

        logger.LogInformation(
            "Handled order {OrderId} for {CustomerId} on delivery {Delivery}",
            order.OrderId, order.CustomerId, attempt + 1);

        return HandlingResult.Success();
    }
}
