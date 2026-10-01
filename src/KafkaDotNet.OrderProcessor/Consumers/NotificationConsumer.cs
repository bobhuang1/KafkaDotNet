using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.OrderProcessor.Consumers;

/// <summary>
/// A second reader of the very same <see cref="OrderTopics.Placed"/> topic, in its
/// own consumer group. It sends the "order received" confirmation a customer sees.
///
/// This is the difference between Kafka and a classic queue: the processor and the
/// notifier each get *every* order, at their own pace, because a consumer group
/// owns its own offsets. A queue would have handed each message to exactly one of
/// them. Because the group is new, Kafka replays the topic from the start for it —
/// no producer change required.
/// </summary>
public sealed class NotificationConsumer : BackgroundService
{
    private const string NotificationGroup = "order-notifications";

    private readonly IServiceProvider _services;
    private readonly KafkaOptions _options;
    private readonly ILogger<NotificationConsumer> _logger;

    public NotificationConsumer(
        IServiceProvider services,
        IOptions<KafkaOptions> options,
        ILogger<NotificationConsumer> logger)
    {
        _services = services;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = _services.BuildConsumer<OrderPlaced>(NotificationGroup);
        consumer.Subscribe(OrderTopics.Placed);

        _logger.LogInformation("Consuming {Topic} as group {Group}", OrderTopics.Placed, NotificationGroup);

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
                _logger.LogError(ex, "Skipping a notification record that could not be deserialized");
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

            var order = result.Message.Value;
            _logger.LogInformation(
                "Notification: thank you {CustomerId}, we received order {OrderId} for {TotalAmount}. Reply STOP to opt out.",
                order.CustomerId, order.OrderId, order.TotalAmount);

            consumer.Commit(result);
        }

        consumer.Close();
        _logger.LogInformation("Stopped consuming notifications from {Topic}", OrderTopics.Placed);
    }
}
