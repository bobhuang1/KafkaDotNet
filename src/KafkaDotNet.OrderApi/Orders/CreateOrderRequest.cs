namespace KafkaDotNet.OrderApi;

/// <summary>Body of <c>POST /orders</c>.</summary>
/// <param name="CustomerId">Who is ordering. Also the Kafka partition key.</param>
/// <param name="Items">At least one line item.</param>
public sealed record CreateOrderRequest(string? CustomerId, List<CreateOrderItem>? Items)
{
    /// <summary>Collect every problem at once, keyed by field, for a validation response.</summary>
    public Dictionary<string, string[]> Validate()
    {
        var problems = new Dictionary<string, string[]>();

        if (string.IsNullOrWhiteSpace(CustomerId))
        {
            problems["customerId"] = ["A customer id is required; it is also the partition key."];
        }

        if (Items is null || Items.Count == 0)
        {
            problems["items"] = ["An order needs at least one item."];
            return problems;
        }

        var itemErrors = new List<string>();
        for (var i = 0; i < Items.Count; i++)
        {
            var item = Items[i];
            if (string.IsNullOrWhiteSpace(item.Sku))
            {
                itemErrors.Add($"items[{i}].sku is required.");
            }

            if (item.Quantity <= 0)
            {
                itemErrors.Add($"items[{i}].quantity must be greater than zero.");
            }

            if (item.UnitPrice < 0)
            {
                itemErrors.Add($"items[{i}].unitPrice cannot be negative.");
            }
        }

        if (itemErrors.Count > 0)
        {
            problems["items"] = [.. itemErrors];
        }

        return problems;
    }
}

/// <summary>One requested line item, before pricing is trusted.</summary>
public sealed record CreateOrderItem(string Sku, int Quantity, decimal UnitPrice);
