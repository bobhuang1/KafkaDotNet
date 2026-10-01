using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.OrderProcessor.Consumers;

/// <summary>
/// The main pipeline consumer: reads <see cref="OrderTopics.Placed"/> and hands
/// each delivery to <see cref="ConsumedRecordHandler"/>.
///
/// Several instances of this worker with the same group id would split the
/// partitions between them — up to one consumer per partition — which is how the
/// pipeline scales out without any coordination code here.
/// </summary>
public sealed class OrderPlacedConsumer : BackgroundService
{
    private readonly IServiceProvider _services;
    private readonly KafkaOptions _options;
    private readonly ConsumedRecordHandler _handler;
    private readonly ILogger<OrderPlacedConsumer> _logger;

    public OrderPlacedConsumer(
        IServiceProvider services,
        IOptions<KafkaOptions> options,
        ConsumedRecordHandler handler,
        ILogger<OrderPlacedConsumer> logger)
    {
        _services = services;
        _options = options.Value;
        _handler = handler;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = _services.BuildConsumer<OrderPlaced>(_options.ConsumerGroup);
        consumer.Subscribe(OrderTopics.Placed);

        _logger.LogInformation("Consuming {Topic} as group {Group}", OrderTopics.Placed, _options.ConsumerGroup);

        // Yield before the blocking poll loop so host startup is not held up.
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, OrderPlaced>? result;
            try
            {
                result = consumer.Consume(TimeSpan.FromMilliseconds(_options.PollTimeoutMs));
            }
            catch (ConsumeException ex) when (ConsumeErrors.IsPoison(ex))
            {
                _logger.LogError(ex, "Skipping a record that could not be deserialized");
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
                // The offset is deliberately left uncommitted. Kafka does not
                // redeliver within the same session, so the next restart replays
                // from here — which is the at-least-once contract in action.
                _logger.LogError(ex,
                    "Could not process order at {Topic}[{Partition}] offset {Offset}; leaving the offset uncommitted",
                    result.Topic, result.Partition.Value, result.Offset.Value);
            }
        }

        consumer.Close();
        _logger.LogInformation("Stopped consuming {Topic}", OrderTopics.Placed);
    }
}
