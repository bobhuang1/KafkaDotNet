using Confluent.Kafka;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class KafkaConfigFactoryTests
{
    private static readonly KafkaOptions Options = new()
    {
        BootstrapServers = "broker:9092",
        ClientId = "unit-test",
    };

    [Fact]
    public void Producer_waits_for_all_replicas_and_is_idempotent()
    {
        var config = KafkaConfigFactory.Producer(Options);

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal("unit-test", config.ClientId);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
    }

    [Fact]
    public void Consumer_commits_by_hand_and_starts_from_the_beginning()
    {
        var config = KafkaConfigFactory.Consumer(Options, "group-x");

        Assert.Equal("group-x", config.GroupId);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
    }

    [Fact]
    public void Consumer_requires_a_group()
        => Assert.Throws<ArgumentException>(() => KafkaConfigFactory.Consumer(Options, "  "));

    [Fact]
    public void Admin_uses_the_same_brokers()
        => Assert.Equal("broker:9092", KafkaConfigFactory.Admin(Options).BootstrapServers);
}
