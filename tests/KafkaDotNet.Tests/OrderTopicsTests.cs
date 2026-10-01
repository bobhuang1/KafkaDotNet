using KafkaDotNet.Contracts;

namespace KafkaDotNet.Tests;

public sealed class OrderTopicsTests
{
    [Fact]
    public void Every_topic_is_declared_for_creation()
    {
        var names = OrderTopics.All.Select(t => t.Name).ToArray();

        Assert.Contains(OrderTopics.Placed, names);
        Assert.Contains(OrderTopics.Confirmed, names);
        Assert.Contains(OrderTopics.Rejected, names);
        Assert.Contains(OrderTopics.Retry, names);
        Assert.Contains(OrderTopics.DeadLetter, names);
    }

    [Fact]
    public void Topic_names_are_unique()
    {
        var names = OrderTopics.All.Select(t => t.Name).ToList();
        Assert.Equal(names.Count, names.Distinct().Count());
    }

    [Fact]
    public void Every_topic_has_at_least_one_partition()
        => Assert.All(OrderTopics.All, topic => Assert.True(topic.Partitions >= 1));

    [Fact]
    public void Dead_letter_name_follows_the_convention()
        => Assert.Equal("orders.placed.dlq", OrderTopics.DeadLetterFor(OrderTopics.Placed));
}
