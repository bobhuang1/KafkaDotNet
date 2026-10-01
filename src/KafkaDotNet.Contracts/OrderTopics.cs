namespace KafkaDotNet.Contracts;

/// <summary>A topic and how it is physically laid out on the broker.</summary>
/// <param name="Name">Topic name.</param>
/// <param name="Partitions">
/// Partition count. Partitions are the unit of parallelism for consumers and the
/// unit of ordering for producers: everything with the same key lands on the same
/// partition and is appended in order.
/// </param>
/// <param name="ReplicationFactor">Copies kept across brokers. One is fine for a single-node demo.</param>
public sealed record TopicDefinition(string Name, int Partitions, short ReplicationFactor = 1);

/// <summary>
/// Every topic this sample touches, in one place. Names are constants so a typo is
/// a compiler error rather than a message silently written to nowhere.
/// </summary>
public static class OrderTopics
{
    /// <summary>Orders accepted by the API. Produced by the API, consumed by the processor.</summary>
    public const string Placed = "orders.placed";

    /// <summary>Orders that passed processing. Produced by the processor, consumed downstream.</summary>
    public const string Confirmed = "orders.confirmed";

    /// <summary>
    /// Orders that were processed and deliberately declined (out of stock, bad address).
    /// This is a business outcome, not a failure — nothing here needs retrying.
    /// </summary>
    public const string Rejected = "orders.rejected";

    /// <summary>Transient failures waiting to be tried again. Same key, so it keeps its partition.</summary>
    public const string Retry = "orders.placed.retry";

    /// <summary>Messages nobody could process. Kept for inspection instead of being dropped.</summary>
    public const string DeadLetter = "orders.placed.dlq";

    /// <summary>Everything to create on startup, in creation order.</summary>
    public static IReadOnlyList<TopicDefinition> All { get; } =
    [
        new(Placed, Partitions: 3),
        new(Confirmed, Partitions: 3),
        new(Rejected, Partitions: 1),
        new(Retry, Partitions: 3),
        // The dead-letter topic is read by a human, so a single partition is enough.
        new(DeadLetter, Partitions: 1),
    ];

    /// <summary>Conventional dead-letter name for a topic: <c>orders.placed</c> becomes <c>orders.placed.dlq</c>.</summary>
    public static string DeadLetterFor(string topic) => topic + ".dlq";
}
