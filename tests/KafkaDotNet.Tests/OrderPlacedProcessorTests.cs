using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using KafkaDotNet.OrderProcessor.Orders;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.Tests;

public sealed class OrderPlacedProcessorTests
{
    private static readonly RetryPolicy Policy = new()
    {
        MaxAttempts = 3,
        InitialDelay = TimeSpan.FromSeconds(2),
        BackoffMultiplier = 2.0,
        MaxDelay = TimeSpan.FromSeconds(15),
    };

    private static OrderPlaced Order(decimal total = 25m) => new(
        Guid.NewGuid(), "customer-1", [new OrderItem("SKU-001", 1, total)], total, DateTimeOffset.UtcNow);

    private static (OrderPlacedProcessor Processor, RecordingPublisher<OrderConfirmed> Confirmed,
        RecordingPublisher<OrderRejected> Rejected, RecordingPublisher<OrderPlaced> Placed)
        Build(IOrderHandler handler)
    {
        var confirmed = new RecordingPublisher<OrderConfirmed>();
        var rejected = new RecordingPublisher<OrderRejected>();
        var placed = new RecordingPublisher<OrderPlaced>();

        var processor = new OrderPlacedProcessor(
            handler,
            confirmed,
            rejected,
            placed,
            Options.Create(new KafkaOptions { Retry = Policy }),
            NullLogger<OrderPlacedProcessor>.Instance);

        return (processor, confirmed, rejected, placed);
    }

    [Fact]
    public async Task Success_publishes_a_confirmation_and_nothing_else()
    {
        var (processor, confirmed, rejected, placed) = Build(new FakeHandler((_, _) => HandlingResult.Success()));

        var result = await processor.ProcessAsync("customer-1", Order(), attempt: 0, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Confirmed, result.Outcome);
        var published = Assert.Single(confirmed.Published);
        Assert.Equal(OrderTopics.Confirmed, published.Topic);
        Assert.Empty(rejected.Published);
        Assert.Empty(placed.Published);
    }

    [Fact]
    public async Task A_permanent_failure_is_rejected_without_any_retry()
    {
        var (processor, confirmed, rejected, placed) = Build(
            new FakeHandler((_, _) => HandlingResult.PermanentFailure("SKU gone")));

        var result = await processor.ProcessAsync("customer-1", Order(), attempt: 0, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Rejected, result.Outcome);
        Assert.Equal("SKU gone", result.Reason);
        var published = Assert.Single(rejected.Published);
        Assert.Equal(OrderTopics.Rejected, published.Topic);
        Assert.Empty(confirmed.Published);
        Assert.Empty(placed.Published); // never touches the retry or dead-letter topic
    }

    [Fact]
    public async Task A_transient_failure_is_rescheduled_on_the_retry_topic()
    {
        var (processor, confirmed, rejected, placed) = Build(
            new FakeHandler((_, _) => HandlingResult.Transient("timeout")));

        var result = await processor.ProcessAsync("customer-1", Order(), attempt: 0, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Retried, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(2), result.RetryDelay);

        var published = Assert.Single(placed.Published);
        Assert.Equal(OrderTopics.Retry, published.Topic);
        Assert.Equal("customer-1", published.Key);      // key preserved: same partition
        Assert.Equal(1, published.Attempt);             // first retry
        Assert.Equal("timeout", published.FailureReason);
        Assert.NotNull(published.NotBefore);
        Assert.Empty(confirmed.Published);
        Assert.Empty(rejected.Published);
    }

    [Fact]
    public async Task Retry_delay_keeps_growing_until_attempts_run_out()
    {
        var (processor, _, _, placed) = Build(new FakeHandler((_, _) => HandlingResult.Transient("still down")));

        var first = await processor.ProcessAsync("customer-1", Order(), attempt: 0, CancellationToken.None);
        var second = await processor.ProcessAsync("customer-1", Order(), attempt: 1, CancellationToken.None);
        var third = await processor.ProcessAsync("customer-1", Order(), attempt: 2, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Retried, first.Outcome);
        Assert.Equal(ProcessingOutcome.Retried, second.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(4), second.RetryDelay);
        Assert.Equal(ProcessingOutcome.DeadLettered, third.Outcome); // 3rd failure == MaxAttempts

        // Two retries then a dead letter: the retry schedule runs out exactly at MaxAttempts.
        Assert.Equal(3, placed.Published.Count);
        Assert.Equal([OrderTopics.Retry, OrderTopics.Retry, OrderTopics.DeadLetter], placed.Published.Select(p => p.Topic));
        Assert.Equal([1, 2, 3], placed.Published.Select(p => p.Attempt));
    }

    [Fact]
    public async Task Exhausted_attempts_move_the_event_to_the_dead_letter_topic()
    {
        var (processor, confirmed, rejected, placed) = Build(
            new FakeHandler((_, _) => HandlingResult.Transient("kaboom")));

        // attempt == 2 means this is the third delivery, and MaxAttempts is 3.
        var result = await processor.ProcessAsync("customer-1", Order(), attempt: 2, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.DeadLettered, result.Outcome);

        var published = Assert.Single(placed.Published);
        Assert.Equal(OrderTopics.DeadLetter, published.Topic);
        Assert.Equal(3, published.Attempt);
        Assert.Equal("kaboom", published.FailureReason);
        Assert.Null(published.NotBefore);   // dead letters are not delayed
        Assert.Empty(confirmed.Published);
        Assert.Empty(rejected.Published);
    }

    [Fact]
    public async Task An_unexpected_exception_is_treated_as_transient()
    {
        var (processor, _, _, placed) = Build(new ThrowingHandler());

        var result = await processor.ProcessAsync("customer-1", Order(), attempt: 0, CancellationToken.None);

        Assert.Equal(ProcessingOutcome.Retried, result.Outcome);
        var published = Assert.Single(placed.Published);
        Assert.Equal(OrderTopics.Retry, published.Topic);
        Assert.Contains("InvalidOperationException", published.FailureReason);
    }

    private sealed class FakeHandler(Func<OrderPlaced, int, HandlingResult> behaviour) : IOrderHandler
    {
        public Task<HandlingResult> HandleAsync(OrderPlaced order, int attempt, CancellationToken cancellationToken)
            => Task.FromResult(behaviour(order, attempt));
    }

    private sealed class ThrowingHandler : IOrderHandler
    {
        public Task<HandlingResult> HandleAsync(OrderPlaced order, int attempt, CancellationToken cancellationToken)
            => throw new InvalidOperationException("handler blew up");
    }
}
