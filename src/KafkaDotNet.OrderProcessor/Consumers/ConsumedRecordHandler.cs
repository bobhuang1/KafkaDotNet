using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using KafkaDotNet.OrderProcessor.Orders;

namespace KafkaDotNet.OrderProcessor.Consumers;

/// <summary>
/// Wraps one delivery: run it through the processor, then commit the offset.
/// The order of those two steps is the whole delivery guarantee — committing
/// first would make the pipeline at-most-once and lose orders on a crash.
/// </summary>
public sealed class ConsumedRecordHandler(OrderPlacedProcessor processor, ILogger<ConsumedRecordHandler> logger)
{
    /// <summary>Process the record and, on success, commit its offset.</summary>
    public async Task HandleAsync(
        IConsumer<string, OrderPlaced> consumer,
        ConsumeResult<string, OrderPlaced> result,
        CancellationToken cancellationToken)
    {
        var order = result.Message.Value;
        var attempt = result.Message.Headers.GetAttempt();

        var outcome = await processor.ProcessAsync(result.Message.Key, order, attempt, cancellationToken).ConfigureAwait(false);

        // Only now is it safe to move the committed offset forward.
        consumer.Commit(result);

        logger.LogInformation(
            "Committed {Topic}[{Partition}] offset {Offset} for order {OrderId}: {Outcome}",
            result.Topic, result.Partition.Value, result.Offset.Value, order.OrderId, outcome.Outcome);
    }
}
