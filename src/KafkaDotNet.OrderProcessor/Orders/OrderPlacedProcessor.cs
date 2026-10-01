using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.OrderProcessor.Orders;

/// <summary>Where an order ended up after a delivery attempt.</summary>
public enum ProcessingOutcome
{
    /// <summary>Processed; an <see cref="OrderConfirmed"/> was published.</summary>
    Confirmed,

    /// <summary>Declined for a business reason; an <see cref="OrderRejected"/> was published.</summary>
    Rejected,

    /// <summary>Failed transiently and was rescheduled on the retry topic.</summary>
    Retried,

    /// <summary>Out of attempts; the original event was parked on the dead-letter topic.</summary>
    DeadLettered,
}

/// <summary>The routing decision for one delivery, returned for logging and tests.</summary>
/// <param name="Outcome">Where the order ended up.</param>
/// <param name="RetryDelay">Set when <see cref="Outcome"/> is <see cref="ProcessingOutcome.Retried"/>.</param>
/// <param name="Reason">Failure text, when there was one.</param>
public sealed record ProcessingResult(ProcessingOutcome Outcome, TimeSpan? RetryDelay = null, string? Reason = null);

/// <summary>
/// The heart of the worker: takes one delivery attempt and decides what happens
/// next. Everything here is expressed against <see cref="IEventPublisher{TValue}"/>,
/// so the four branches can be tested without a broker.
/// </summary>
public sealed class OrderPlacedProcessor
{
    private readonly IOrderHandler _handler;
    private readonly IEventPublisher<OrderConfirmed> _confirmed;
    private readonly IEventPublisher<OrderRejected> _rejected;
    private readonly IEventPublisher<OrderPlaced> _placed;
    private readonly RetryPolicy _retryPolicy;
    private readonly ILogger<OrderPlacedProcessor> _logger;

    public OrderPlacedProcessor(
        IOrderHandler handler,
        IEventPublisher<OrderConfirmed> confirmed,
        IEventPublisher<OrderRejected> rejected,
        IEventPublisher<OrderPlaced> placed,
        IOptions<KafkaOptions> options,
        ILogger<OrderPlacedProcessor> logger)
    {
        _handler = handler;
        _confirmed = confirmed;
        _rejected = rejected;
        _placed = placed;
        _retryPolicy = options.Value.Retry;
        _logger = logger;
    }

    /// <summary>
    /// Handle one delivery of an order and publish whatever follows from it.
    /// <paramref name="attempt"/> is the delivery attempt number from the record
    /// headers: zero is the original delivery, one is the first retry.
    /// </summary>
    public async Task<ProcessingResult> ProcessAsync(
        string key,
        OrderPlaced order,
        int attempt,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(order);

        HandlingResult handled;
        try
        {
            handled = await _handler.HandleAsync(order, attempt, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // An unexpected exception is treated as transient, not as a reason to
            // give up: it still goes through the retry schedule and is finally
            // parked on the dead-letter topic rather than vanishing.
            handled = HandlingResult.Transient($"{ex.GetType().Name}: {ex.Message}");
        }

        if (handled.Succeeded)
        {
            var confirmed = new OrderConfirmed(order.OrderId, order.CustomerId, order.TotalAmount, DateTimeOffset.UtcNow);
            await _confirmed.PublishAsync(OrderTopics.Confirmed, key, confirmed, attempt, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return new ProcessingResult(ProcessingOutcome.Confirmed);
        }

        if (handled.Permanent)
        {
            var rejected = new OrderRejected(order.OrderId, order.CustomerId, handled.Reason ?? "rejected", DateTimeOffset.UtcNow);
            await _rejected.PublishAsync(OrderTopics.Rejected, key, rejected, attempt,
                failureReason: handled.Reason, cancellationToken: cancellationToken).ConfigureAwait(false);

            _logger.LogWarning("Order {OrderId} rejected permanently: {Reason}", order.OrderId, handled.Reason);
            return new ProcessingResult(ProcessingOutcome.Rejected, Reason: handled.Reason);
        }

        // Transient: retry with backoff, or park it once attempts are exhausted.
        var failureCount = attempt + 1;
        if (_retryPolicy.ShouldRetry(failureCount))
        {
            var delay = _retryPolicy.DelayFor(failureCount);
            await _placed.PublishAsync(
                OrderTopics.Retry, key, order,
                attempt: failureCount,
                notBefore: DateTimeOffset.UtcNow + delay,
                failureReason: handled.Reason,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _logger.LogWarning(
                "Order {OrderId} failed delivery {Delivery}; retrying in {Delay}: {Reason}",
                order.OrderId, failureCount, delay, handled.Reason);

            return new ProcessingResult(ProcessingOutcome.Retried, delay, handled.Reason);
        }

        await _placed.PublishAsync(
            OrderTopics.DeadLetter, key, order,
            attempt: failureCount,
            failureReason: handled.Reason,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogError(
            "Order {OrderId} exhausted {MaxAttempts} deliveries; moved to {DeadLetter}: {Reason}",
            order.OrderId, _retryPolicy.MaxAttempts, OrderTopics.DeadLetter, handled.Reason);

        return new ProcessingResult(ProcessingOutcome.DeadLettered, Reason: handled.Reason);
    }
}
