using System.Text.Json;
using Confluent.Kafka;
using KafkaDotNet.Contracts;

namespace KafkaDotNet.Messaging;

/// <summary>
/// Thrown when a record on the wire cannot be turned back into an event. Kept
/// distinct from <see cref="JsonException"/> so the consumer loop can tell a
/// "poison message" apart from a network or broker failure — the first should go
/// straight to the dead-letter topic, the second should be retried.
/// </summary>
public sealed class EventDeserializationException : Exception
{
    public EventDeserializationException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Reads and writes events as JSON using <see cref="JsonDefaults.Options"/>.
/// Implements both Confluent interfaces so the same instance plugs into a
/// <c>ProducerBuilder</c> and a <c>ConsumerBuilder</c>, which is what keeps the
/// producer and consumer from drifting apart.
/// </summary>
public sealed class JsonEventSerializer<T> : ISerializer<T>, IDeserializer<T> where T : class
{
    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(JsonSerializerOptions? options = null) => _options = options ?? JsonDefaults.Options;

    /// <inheritdoc />
    public byte[] Serialize(T data, SerializationContext context)
        => data is null
            ? []
            : JsonSerializer.SerializeToUtf8Bytes(data, _options);

    /// <inheritdoc />
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
        {
            throw new EventDeserializationException(
                $"Empty Kafka record on topic '{context.Topic}'; expected JSON for {typeof(T).Name}.");
        }

        try
        {
            return JsonSerializer.Deserialize<T>(data, _options)
                ??                throw new EventDeserializationException(
                    $"Kafka record on topic '{context.Topic}' deserialized to null; expected {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            throw new EventDeserializationException(
                $"Kafka record on topic '{context.Topic}' is not valid {typeof(T).Name} JSON: {ex.Message}",
                ex);
        }
    }
}
