using System.Diagnostics;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.OrderApi;

/// <summary>
/// The producer side of the sample. It accepts an order, turns it into an
/// <see cref="OrderPlaced"/> event, and hands it to Kafka. It deliberately does
/// no inventory or payment work itself: that belongs to the consumer, and keeping
/// the two apart is the whole point of an event-driven design.
/// </summary>
public static class OrderEndpoints
{
    /// <summary>Map <c>/</c>, <c>POST /orders</c> and <c>POST /orders/demo</c>.</summary>
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder routes)
    {
        routes.MapGet("/", () => Results.Ok(new
        {
            service = "KafkaDotNet.OrderApi",
            description = "Publishes OrderPlaced events to Kafka. A separate worker does the processing.",
            endpoints = new[]
            {
                "POST /orders        — publish one order (body: { customerId, items: [{ sku, quantity, unitPrice }] })",
                "POST /orders/demo   — publish ?count= demo orders, including one that will be retried and one that will be rejected",
            },
        }));

        var orders = routes.MapGroup("/orders");
        orders.MapPost("/", CreateOrderAsync).WithName("CreateOrder");
        orders.MapPost("/demo", CreateDemoOrdersAsync).WithName("CreateDemoOrders");

        return routes;
    }

    private static async Task<IResult> CreateOrderAsync(
        CreateOrderRequest request,
        OrderEventPublisher publisher,
        CancellationToken cancellationToken)
    {
        var problems = request.Validate();
        if (problems.Count > 0)
        {
            return Results.ValidationProblem(problems);
        }

        var order = BuildOrder(request.CustomerId!, request.Items!);

        // The trace id from the incoming request is copied onto the Kafka record,
        // so the worker's logs can be joined to the API's for the same order.
        var receipt = await publisher.PublishAsync(order, Activity.Current?.Id, cancellationToken);

        return Results.Accepted($"/orders/{order.OrderId}", new
        {
            order.OrderId,
            order.CustomerId,
            order.TotalAmount,
            receipt.Topic,
            partition = receipt.Partition.Value,
            offset = receipt.Offset.Value,
        });
    }

    private static async Task<IResult> CreateDemoOrdersAsync(
        int? count,
        OrderEventPublisher publisher,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken)
    {
        var logger = loggerFactory.CreateLogger("Orders.Demo");
        var total = Math.Clamp(count ?? 5, 1, 50);
        var published = new List<object>(total);

        for (var i = 0; i < total; i++)
        {
            var order = BuildDemoOrder(i, total);

            // Deliberately bad payload: the worker's handler declines it, so it
            // lands on orders.rejected rather than being retried.
            var receipt = await publisher.PublishAsync(order, traceId: $"demo-{i}", cancellationToken);

            logger.LogInformation("Demo order {Index}/{Total} {OrderId} -> {Topic}[{Partition}]",
                i + 1, total, order.OrderId, receipt.Topic, receipt.Partition.Value);

            published.Add(new { order.OrderId, order.CustomerId, order.TotalAmount, partition = receipt.Partition.Value });
        }

        return Results.Ok(new { published = published.Count, orders = published });
    }

    private static OrderPlaced BuildOrder(string customerId, IReadOnlyList<CreateOrderItem> items)
    {
        var domainItems = items
            .Select(item => new OrderItem(item.Sku.Trim(), item.Quantity, item.UnitPrice))
            .ToArray();

        return new OrderPlaced(
            Guid.NewGuid(),
            customerId.Trim(),
            domainItems,
            domainItems.Sum(item => item.LineTotal),
            DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// A deterministic spread of demo orders, chosen so every branch of the
    /// worker's routing logic is reachable in one call:
    /// index 0 carries a SKU the handler permanently rejects, index 1 is over the
    /// large-order threshold and fails once before succeeding, the rest succeed.
    /// Customers repeat with a period of three so partitioning by customer is visible.
    /// </summary>
    private static OrderPlaced BuildDemoOrder(int index, int total)
    {
        var customer = $"customer-{index % 3}";
        var isRejectable = index == 0;
        var isLarge = index == 1 && total > 1;

        var sku = isRejectable ? "OUT-OF-STOCK" : $"SKU-{index:D3}";
        var unitPrice = isLarge ? 6_000m : 10m + index;
        var quantity = isLarge ? 2 : 1;

        var items = new[] { new OrderItem(sku, quantity, unitPrice) };

        return new OrderPlaced(
            Guid.NewGuid(),
            customer,
            items,
            items.Sum(item => item.LineTotal),
            DateTimeOffset.UtcNow);
    }
}
