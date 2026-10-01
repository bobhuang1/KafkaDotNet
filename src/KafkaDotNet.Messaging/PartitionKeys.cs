namespace KafkaDotNet.Messaging;

/// <summary>
/// The message key decides which partition a record lands on, and therefore what
/// is ordered with respect to what. This is the single most important design
/// decision in a Kafka pipeline, so the choices are named rather than inlined.
/// </summary>
public static class PartitionKeys
{
    /// <summary>
    /// Key orders by customer. Every event for one customer is appended to the same
    /// partition in the order it was produced, so a consumer sees that customer's
    /// orders in sequence. Different customers spread across partitions and are
    /// processed in parallel.
    /// </summary>
    public static string ForCustomer(string customerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(customerId);
        return customerId.Trim().ToLowerInvariant();
    }

    /// <summary>Key by order when per-order ordering is all that matters (e.g. the retry topic).</summary>
    public static string ForOrder(Guid orderId) => orderId.ToString("N");

    /// <summary>
    /// A deterministic non-negative partition for a key, mirroring what the default
    /// partitioner does well enough to reason about in tests and logs.
    /// </summary>
    public static int PartitionFor(string key, int partitionCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);

        // FNV-1a: stable across processes and platforms, unlike string.GetHashCode.
        unchecked
        {
            const uint offsetBasis = 2166136261;
            const uint prime = 16777619;
            var hash = offsetBasis;
            foreach (var b in System.Text.Encoding.UTF8.GetBytes(key))
            {
                hash ^= b;
                hash *= prime;
            }
            return (int)(hash % (uint)partitionCount);
        }
    }
}
