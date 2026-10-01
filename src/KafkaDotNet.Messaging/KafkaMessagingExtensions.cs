using Confluent.Kafka;
using KafkaDotNet.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.Messaging;

/// <summary>
/// Wires the Kafka clients into the host. The API and the worker share this, so
/// both get identical producer settings and the same topic bootstrapper.
/// </summary>
public static class KafkaMessagingExtensions
{
    /// <summary>
    /// Register options, the JSON serializer/deserializer, an admin client, one
    /// producer per event type, the publishers, and the startup topic check.
    /// </summary>
    public static IServiceCollection AddKafkaMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        // Open generic registrations: asking for ISerializer<OrderConfirmed>
        // transparently builds a JsonEventSerializer<OrderConfirmed>.
        services.AddSingleton(typeof(ISerializer<>), typeof(JsonEventSerializer<>));
        services.AddSingleton(typeof(IDeserializer<>), typeof(JsonEventSerializer<>));

        services.AddSingleton<IAdminClient>(sp =>
            new AdminClientBuilder(KafkaConfigFactory.Admin(sp.GetRequiredService<IOptions<KafkaOptions>>().Value)).Build());

        AddProducer<OrderPlaced>(services);
        AddProducer<OrderConfirmed>(services);
        AddProducer<OrderRejected>(services);

        services.AddSingleton(typeof(KafkaEventPublisher<>));
        services.AddSingleton(typeof(IEventPublisher<>), typeof(KafkaEventPublisher<>));
        services.AddSingleton<OrderEventPublisher>();
        services.AddHostedService<TopicBootstrapper>();

        return services;
    }

    /// <summary>
    /// Register a producer for one event type. <c>Acks.All</c> and idempotence are
    /// set in <see cref="KafkaConfigFactory.Producer"/>; a producer-level error
    /// handler logs asynchronously reported failures that never reach a caller.
    /// </summary>
    public static IServiceCollection AddProducer<TValue>(this IServiceCollection services) where TValue : class
    {
        services.AddSingleton<IProducer<string, TValue>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var serializer = sp.GetRequiredService<ISerializer<TValue>>();
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger($"Kafka.Producer.{typeof(TValue).Name}");

            return new ProducerBuilder<string, TValue>(KafkaConfigFactory.Producer(options))
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(serializer)
                .SetErrorHandler((_, error) =>
                    logger.LogError("Kafka producer error {Code}: {Reason} (fatal: {Fatal})", error.Code, error.Reason, error.IsFatal))
                // Route librdkafka's internal chatter through ILogger instead of the
                // raw stderr spew it writes by default.
                .SetLogHandler((_, message) =>
                    logger.LogDebug("Kafka {Level} {Facility}/{Name}: {Message}",
                        message.Level, message.Facility, message.Name, message.Message))
                .Build();
        });

        return services;
    }
}

/// <summary>
/// Builds consumers. Consumers are not singletons: each long-running worker owns
/// exactly one, and a consumer is not thread-safe.
/// </summary>
public static class KafkaConsumerFactory
{
    /// <summary>Build a consumer for one event type in the given group.</summary>
    public static IConsumer<string, TValue> BuildConsumer<TValue>(this IServiceProvider services, string groupId)
        where TValue : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        var options = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var deserializer = services.GetRequiredService<IDeserializer<TValue>>();

        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("Kafka.Consumer");

        return new ConsumerBuilder<string, TValue>(KafkaConfigFactory.Consumer(options, groupId))
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(deserializer)
            .SetErrorHandler((_, error) =>
                logger.LogError("Kafka consumer error {Code}: {Reason} (fatal: {Fatal})", error.Code, error.Reason, error.IsFatal))
            .SetLogHandler((_, message) =>
                logger.LogDebug("Kafka {Level} {Facility}/{Name}: {Message}",
                    message.Level, message.Facility, message.Name, message.Message))
            .Build();
    }
}
