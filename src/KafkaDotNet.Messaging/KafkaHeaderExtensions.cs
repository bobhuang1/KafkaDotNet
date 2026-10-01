using System.Globalization;
using System.Text;
using Confluent.Kafka;
using KafkaDotNet.Contracts;

namespace KafkaDotNet.Messaging;

/// <summary>
/// Small typed helpers over the raw <see cref="Headers"/> collection. Kafka headers
/// are byte arrays, so without these every call site would repeat the same UTF-8
/// encoding and integer parsing.
/// </summary>
public static class KafkaHeaderExtensions
{
    /// <summary>Write (or replace) a UTF-8 string header. A null value removes the header.</summary>
    public static void SetString(this Headers headers, string key, string? value)
    {
        ArgumentNullException.ThrowIfNull(headers);
        headers.Remove(key);
        if (value is not null)
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }
    }

    /// <summary>Write an integer header in the invariant culture, so it reads the same everywhere.</summary>
    public static void SetLong(this Headers headers, string key, long value)
        => headers.SetString(key, value.ToString(CultureInfo.InvariantCulture));

    /// <summary>Read a UTF-8 string header, or null when it is absent.</summary>
    public static string? GetString(this Headers headers, string key)
    {
        ArgumentNullException.ThrowIfNull(headers);
        return headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
    }

    /// <summary>Read an integer header, or null when absent or unparseable.</summary>
    public static long? GetLong(this Headers headers, string key)
        => long.TryParse(headers.GetString(key), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    /// <summary>
    /// Delivery attempt, defaulting to zero. A message with no attempt header has
    /// never been retried, which is exactly what a producer should emit.
    /// </summary>
    public static int GetAttempt(this Headers headers)
        => (int)Math.Clamp(headers.GetLong(EventHeaders.Attempt) ?? 0, 0, int.MaxValue);

    /// <summary>The time before which a retry must not be delivered, if it is set.</summary>
    public static DateTimeOffset? GetNotBefore(this Headers headers)
    {
        var raw = headers.GetLong(EventHeaders.NotBefore);
        return raw is null ? null : DateTimeOffset.FromUnixTimeMilliseconds(raw.Value);
    }

    /// <summary>Stamp the "do not deliver before" time used by the retry consumer.</summary>
    public static void SetNotBefore(this Headers headers, DateTimeOffset notBefore)
        => headers.SetLong(EventHeaders.NotBefore, notBefore.ToUnixTimeMilliseconds());
}
