using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class PartitionKeysTests
{
    [Theory]
    [InlineData("Customer-1", "customer-1")]
    [InlineData("  customer-2  ", "customer-2")]
    [InlineData("CUSTOMER-3", "customer-3")]
    public void Customer_keys_are_normalised_so_casing_cannot_split_a_customer(string input, string expected)
        => Assert.Equal(expected, PartitionKeys.ForCustomer(input));

    [Fact]
    public void Customer_key_rejects_blank_input()
        => Assert.Throws<ArgumentException>(() => PartitionKeys.ForCustomer("   "));

    [Fact]
    public void Order_keys_are_stable_for_the_same_guid()
    {
        var id = Guid.NewGuid();
        Assert.Equal(PartitionKeys.ForOrder(id), PartitionKeys.ForOrder(id));
    }

    [Fact]
    public void Partition_is_deterministic_and_in_range()
    {
        var first = PartitionKeys.PartitionFor("customer-1", 3);
        var second = PartitionKeys.PartitionFor("customer-1", 3);

        Assert.Equal(first, second);
        Assert.InRange(first, 0, 2);
    }

    [Fact]
    public void Different_customers_spread_over_partitions()
    {
        var partitions = Enumerable.Range(0, 50)
            .Select(i => PartitionKeys.PartitionFor($"customer-{i}", 8))
            .Distinct()
            .Count();

        // A decent hash should touch more than one partition across 50 keys.
        Assert.True(partitions > 1, $"expected a spread, saw {partitions} partition(s)");
    }

    [Fact]
    public void Partition_rejects_a_non_positive_partition_count()
        => Assert.Throws<ArgumentOutOfRangeException>(() => PartitionKeys.PartitionFor("k", 0));
}
