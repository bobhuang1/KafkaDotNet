using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaDotNet.Contracts;

/// <summary>
/// One JSON configuration for the whole system. Every producer and consumer uses
/// these options, which is what makes the wire format stable: if the API and the
/// worker disagreed about casing or enum handling, messages would round-trip
/// "successfully" into the wrong shape.
/// </summary>
public static class JsonDefaults
{
    /// <summary>
    /// Web defaults: camelCase property names, case-insensitive reading, numbers
    /// stay numbers. Enums are written as readable strings rather than integers so
    /// a message on the broker is legible to a human staring at it in a UI.
    /// </summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };
}
