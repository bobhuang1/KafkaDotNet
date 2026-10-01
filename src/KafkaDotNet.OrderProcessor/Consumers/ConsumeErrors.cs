using Confluent.Kafka;

namespace KafkaDotNet.OrderProcessor.Consumers;

/// <summary>Small classification helpers for the errors a consume loop can see.</summary>
internal static class ConsumeErrors
{
    /// <summary>
    /// True when the record could not be turned into an event. That is a "poison
    /// message": retrying it byte-for-byte will never help, so it is logged and
    /// skipped rather than left to block the partition.
    /// </summary>
    public static bool IsPoison(ConsumeException exception)
        => exception.Error.Code is ErrorCode.Local_ValueDeserialization or ErrorCode.Local_KeyDeserialization;
}
