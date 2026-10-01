namespace KafkaDotNet.Messaging;

/// <summary>
/// Everything the sample reads from the <c>Kafka</c> configuration section.
/// Bound from appsettings.json so the same binary can point at a local
/// docker-compose broker or a real cluster without a rebuild.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "Kafka";

    /// <summary>Comma-separated broker list the client bootstraps from, e.g. <c>localhost:9092</c>.</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Consumer group for the main order pipeline.</summary>
    public string ConsumerGroup { get; set; } = "order-processor";

    /// <summary>Separate group for the retry topic, so retries never block fresh orders.</summary>
    public string RetryConsumerGroup { get; set; } = "order-processor-retry";

    /// <summary>Shown in broker-side client metrics; handy when several apps share a cluster.</summary>
    public string? ClientId { get; set; }

    /// <summary>How long a consumer waits for a record before looping again.</summary>
    public int PollTimeoutMs { get; set; } = 500;

    /// <summary>Retry schedule for transient processing failures.</summary>
    public RetryPolicy Retry { get; set; } = new();
}
