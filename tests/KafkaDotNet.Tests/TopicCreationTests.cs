using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class TopicCreationTests
{
    [Fact]
    public void NoError_means_created()
    {
        var outcomes = TopicCreation.Classify([Report("orders.placed", ErrorCode.NoError)]);

        var outcome = Assert.Single(outcomes);
        Assert.True(outcome.Created);
        Assert.Null(outcome.Error);
    }

    [Fact]
    public void AlreadyExists_is_not_treated_as_a_failure()
    {
        var outcomes = TopicCreation.Classify([Report("orders.placed", ErrorCode.TopicAlreadyExists)]);

        var outcome = Assert.Single(outcomes);
        Assert.False(outcome.Created);
        Assert.Null(outcome.Error);
    }

    [Fact]
    public void Any_other_error_is_surfaced()
    {
        var outcomes = TopicCreation.Classify([Report("orders.placed", ErrorCode.TopicException)]);

        var outcome = Assert.Single(outcomes);
        Assert.False(outcome.Created);
        Assert.NotNull(outcome.Error);
    }

    [Fact]
    public void Every_report_produces_an_outcome()
    {
        var outcomes = TopicCreation.Classify(
        [
            Report("a", ErrorCode.NoError),
            Report("b", ErrorCode.TopicAlreadyExists),
            Report("c", ErrorCode.TopicException),
        ]);

        Assert.Equal(3, outcomes.Count);
        Assert.Equal(["a", "b", "c"], outcomes.Select(o => o.Topic));
    }

    private static CreateTopicReport Report(string topic, ErrorCode code)
        => new() { Topic = topic, Error = new Error(code) };
}
