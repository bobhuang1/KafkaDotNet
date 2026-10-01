using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.OrderProcessor.Consumers;

/// <summary>
/// Reads the retry topic, but not before each record's <c>not-before</c> time.
///
/// Kafka has no delayed delivery, so the wait is implemented here: when the head
/// of a partition is not due yet, the consumer seeks back onto it and pauses the
/// partition. Pausing (rather than sleeping) matters — it leaves the partition's
/// position intact, so once the delay elapses the record is re-read instead of
/// being skipped, and the order within the partition is preserved.
///
/// Retries live in their own topic and their own consumer group so a backlog of
/// retries never delays fresh orders.
/// </summary>
public sealed class RetryConsumer : BackgroundService
{
    private readonly IServiceProvider _services;
    private readonly KafkaOptions _options;
    private readonly ConsumedRecordHandler _handler;
    private readonly ILogger<RetryConsumer> _logger;

    public RetryConsumer(
        IServiceProvider services,
        IOptions<KafkaOptions> options,
        ConsumedRecordHandler handler,
        ILogger<RetryConsumer> logger)
    {
        _services = services;
        _options = options.Value;
        _handler = handler;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = _services.BuildConsumer<OrderPlaced>(_options.RetryConsumerGroup);
        consumer.Subscribe(OrderTopics.Retry);

        _logger.LogInformation("Consuming {Topic} as group {Group}", OrderTopics.Retry, _options.RetryConsumerGroup);

        // Yield before the blocking poll loop so host startup is not held up.
        await Task.Yield();

        // Partitions parked until a delayed record is due.
        var parkedUntil = new Dictionary<TopicPartition, DateTimeOffset>();

        while (!stoppingToken.IsCancellationRequested)
        {
            ResumeDuePartitions(consumer, parkedUntil);

            // Poll briskly while something is parked so the delay is honoured promptly.
            var timeout = TimeSpan.FromMilliseconds(parkedUntil.Count == 0 ? _options.PollTimeoutMs : 200);

            ConsumeResult<string, OrderPlaced>? result;
            try
            {
                result = consumer.Consume(timeout);
            }
            catch (ConsumeException ex) when (ConsumeErrors.IsPoison(ex))
            {
                _logger.LogError(ex, "Skipping a retry record that could not be deserialized");
                continue;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (result?.Message is null)
            {
                continue;
            }

            var notBefore = result.Message.Headers.GetNotBefore();
            if (notBefore is not null && DateTimeOffset.UtcNow < notBefore)
            {
                // Not due yet: hold this record at the head of its partition.
                consumer.Seek(result.TopicPartitionOffset);
                consumer.Pause([result.TopicPartition]);
                parkedUntil[result.TopicPartition] = notBefore.Value;

                _logger.LogInformation(
                    "Order {OrderId} is not due until {NotBefore:O}; pausing {Topic}[{Partition}]",
                    result.Message.Value.OrderId, notBefore.Value, result.Topic, result.Partition.Value);
                continue;
            }

            try
            {
                await _handler.HandleAsync(consumer, result, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Could not process retry record at {Topic}[{Partition}] offset {Offset}",
                    result.Topic, result.Partition.Value, result.Offset.Value);
            }
        }

        consumer.Close();
        _logger.LogInformation("Stopped consuming {Topic}", OrderTopics.Retry);
    }

    private void ResumeDuePartitions(IConsumer<string, OrderPlaced> consumer, Dictionary<TopicPartition, DateTimeOffset> parkedUntil)
    {
        if (parkedUntil.Count == 0)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        foreach (var (partition, due) in parkedUntil.ToArray())
        {
            if (now < due)
            {
                continue;
            }

            consumer.Resume([partition]);
            parkedUntil.Remove(partition);
            _logger.LogInformation("Resuming {Topic}[{Partition}] after its retry delay", partition.Topic, partition.Partition.Value);
        }
    }
}
