using KafkaDotNet.Contracts;
using KafkaDotNet.OrderProcessor.Orders;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaDotNet.Tests;

public sealed class SimulatedOrderHandlerTests
{
    private readonly SimulatedOrderHandler _handler = new(NullLogger<SimulatedOrderHandler>.Instance);

    private static OrderPlaced Order(string sku, decimal total) => new(
        Guid.NewGuid(), "customer-1", [new OrderItem(sku, 1, total)], total, DateTimeOffset.UtcNow);

    [Fact]
    public async Task A_normal_order_succeeds_on_the_first_delivery()
    {
        var result = await _handler.HandleAsync(Order("SKU-001", 25m), attempt: 0, CancellationToken.None);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task An_unavailable_sku_fails_permanently()
    {
        var result = await _handler.HandleAsync(Order(SimulatedOrderHandler.UnavailableSku, 25m), attempt: 0, CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.True(result.Permanent);
    }

    [Fact]
    public async Task A_large_order_fails_once_then_succeeds()
    {
        var order = Order("SKU-002", SimulatedOrderHandler.LargeOrderThreshold + 1);

        var firstAttempt = await _handler.HandleAsync(order, attempt: 0, CancellationToken.None);
        var secondAttempt = await _handler.HandleAsync(order, attempt: 1, CancellationToken.None);

        Assert.False(firstAttempt.Succeeded);
        Assert.False(firstAttempt.Permanent);       // worth retrying
        Assert.True(secondAttempt.Succeeded);
    }
}
