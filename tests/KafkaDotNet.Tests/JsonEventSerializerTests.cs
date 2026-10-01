using System.Text;
using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class JsonEventSerializerTests
{
    private readonly JsonEventSerializer<OrderPlaced> _serializer = new();
    private static readonly SerializationContext Context = new(MessageComponentType.Value, OrderTopics.Placed);

    private static OrderPlaced SampleOrder() => new(
        Guid.Parse("11111111-2222-3333-4444-555555555555"),
        "customer-1",
        [new OrderItem("SKU-001", 2, 10.50m), new OrderItem("SKU-002", 1, 4.00m)],
        25.00m,
        new DateTimeOffset(2026, 9, 30, 12, 0, 0, TimeSpan.Zero));

    [Fact]
    public void Round_trips_an_order_unchanged()
    {
        var original = SampleOrder();

        var bytes = _serializer.Serialize(original, Context);
        var restored = _serializer.Deserialize(bytes, isNull: false, Context);

        Assert.Equal(original.OrderId, restored.OrderId);
        Assert.Equal(original.CustomerId, restored.CustomerId);
        Assert.Equal(original.TotalAmount, restored.TotalAmount);
        Assert.Equal(original.Items.Count, restored.Items.Count);
        Assert.Equal(original.Items[0].Sku, restored.Items[0].Sku);
        Assert.Equal(original.Items[0].LineTotal, restored.Items[0].LineTotal);
    }

    [Fact]
    public void Writes_camelCase_property_names()
    {
        var json = Encoding.UTF8.GetString(_serializer.Serialize(SampleOrder(), Context));

        Assert.Contains("\"orderId\"", json);
        Assert.Contains("\"customerId\"", json);
        Assert.Contains("\"totalAmount\"", json);
        Assert.DoesNotContain("\"OrderId\"", json);
    }

    [Fact]
    public void Empty_payload_is_reported_as_a_deserialization_failure()
    {
        var exception = Assert.Throws<EventDeserializationException>(
            () => _serializer.Deserialize([], isNull: false, Context));

        Assert.Contains(typeof(OrderPlaced).Name, exception.Message);
    }

    [Fact]
    public void Null_payload_is_reported_as_a_deserialization_failure()
    {
        var exception = Assert.Throws<EventDeserializationException>(
            () => _serializer.Deserialize([], isNull: true, Context));

        Assert.Contains(OrderTopics.Placed, exception.Message);
    }

    [Fact]
    public void Malformed_json_is_wrapped_in_a_deserialization_failure()
    {
        var garbage = Encoding.UTF8.GetBytes("{ this is not json");

        var exception = Assert.Throws<EventDeserializationException>(
            () => _serializer.Deserialize(garbage, isNull: false, Context));

        Assert.IsType<System.Text.Json.JsonException>(exception.InnerException);
    }

    [Fact]
    public void Deserializing_null_returns_the_null_object()
    {
        var restored = System.Text.Json.JsonSerializer.Deserialize<OrderPlaced>("null", JsonDefaults.Options);
        Assert.Null(restored);
    }
}
