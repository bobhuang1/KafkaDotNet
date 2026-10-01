namespace KafkaDotNet.Contracts;

/// <summary>
/// Kafka record headers carry the metadata that is *about* a message rather than
/// part of it. Keeping them here (instead of in the payload) means a consumer can
/// route, count retries, or trace a message without deserializing the body — and
/// it means the body stays a plain domain object.
/// </summary>
public static class EventHeaders
{
    /// <summary>Stable id for the event, so a redelivery can be recognised as the same thing.</summary>
    public const string EventId = "event-id";

    /// <summary>The logical event name, e.g. <c>OrderPlaced</c>. Lets one topic carry several shapes.</summary>
    public const string EventType = "event-type";

    /// <summary>Zero-based delivery attempt. Incremented every time the message is retried.</summary>
    public const string Attempt = "attempt";

    /// <summary>Unix milliseconds before which a retry must not be delivered.</summary>
    public const string NotBefore = "not-before";

    /// <summary>Correlation id from the caller, carried through so logs can be joined up.</summary>
    public const string TraceId = "trace-id";

    /// <summary>Why the message was rejected, stamped on the way to the dead-letter topic.</summary>
    public const string FailureReason = "failure-reason";

    /// <summary>ISO-8601 time the event was created.</summary>
    public const string OccurredAt = "occurred-at";
}
