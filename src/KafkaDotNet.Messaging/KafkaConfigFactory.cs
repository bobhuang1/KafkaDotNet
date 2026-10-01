using Confluent.Kafka;

namespace KafkaDotNet.Messaging;

/// <summary>
/// Turns <see cref="KafkaOptions"/> into the Confluent client configs. Kept as
/// pure functions so the *settings that matter* — idempotence, manual commit,
/// earliest reset — are visible in one place and can be asserted in tests.
/// </summary>
public static class KafkaConfigFactory
{
    /// <summary>
    /// Producer settings for the order pipeline.
    ///
    /// <c>Acks.All</c> means the broker confirms a write only once every in-sync
    /// replica has it: a "delivered" result can survive a broker dying.
    /// <c>EnableIdempotence</c> stops the producer's own internal retries from
    /// writing duplicates, so a single <c>ProduceAsync</c> writes at most once.
    /// <c>MessageTimeoutMs</c> bounds the total time a produce call may spend
    /// retrying. The default is five minutes, which turns an unreachable broker
    /// into a hung request; ten seconds fails fast instead.
    /// </summary>
    public static ProducerConfig Producer(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5,
            MessageTimeoutMs = 10_000,
            LingerMs = 5,
        };
    }

    /// <summary>
    /// Consumer settings for the order pipeline.
    ///
    /// <c>EnableAutoCommit = false</c> is the important line. Offsets are committed
    /// by hand *after* the work is done, which is what gives at-least-once delivery:
    /// a crash between doing the work and committing means the message is replayed.
    /// <c>AutoOffsetReset.Earliest</c> means a brand-new group replays the topic from
    /// the start rather than skipping whatever is already there.
    /// </summary>
    public static ConsumerConfig Consumer(KafkaOptions options, string groupId)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        return new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false,
            SessionTimeoutMs = 10_000,
            MaxPollIntervalMs = 300_000,
        };
    }

    /// <summary>Admin client config — used only to create topics on startup.</summary>
    public static AdminClientConfig Admin(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
        };
    }
}
