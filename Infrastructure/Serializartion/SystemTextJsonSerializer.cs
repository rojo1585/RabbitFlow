using RabbitFlow.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RabbitFlow.Infrastructure.Serializartion;


/// <summary>
/// Default message serializer using System.Text.Json.
/// Wraps event payloads in a <see cref="MessageEnvelope"/> for type-safe deserialization.
/// </summary>
/// <remarks>
/// <para>
/// The envelope wraps every message with an <c>EventType</c> field. However, the
/// consumer resolves event types from the <c>x-event-type</c> AMQP header (set by
/// the publisher), not from the envelope. The envelope exists for:
/// <list type="bullet">
///   <item>Backward compatibility with consumers that read the body directly.</item>
///   <item>Human readability when inspecting raw messages.</item>
/// </list>
/// </para>
/// <para>
/// NOTE: The <c>EventVersion</c> in the envelope always defaults to 1. The correct
/// version is in the AMQP header <c>x-event-version</c>. This will be aligned in
/// Phase 13 (Event Versioning) when the wire format is revisited.
/// </para>
/// </remarks>
public sealed class SystemTextJsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Creates a new serializer with default options (camelCase, ignore null values).
    /// </summary>
    public SystemTextJsonSerializer() : this(new JsonSerializerOptions()) { }

    /// <summary>
    /// Creates a new serializer with custom options.
    /// </summary>
    /// <param name="options">JSON serializer options. PropertyNamingPolicy and
    /// DefaultIgnoreCondition can be customized.</param>
    public SystemTextJsonSerializer(JsonSerializerOptions options)
    {
        _options = options;
        _options.PropertyNamingPolicy ??= JsonNamingPolicy.CamelCase;
        if (_options.DefaultIgnoreCondition == default)
            _options.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
    }

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Serialize<T>(T message)
    {
        var envelope = new MessageEnvelope
        {
            EventType = typeof(T).FullName!,
            EventVersion = 1,
            Payload = message,
        };

        var json = JsonSerializer.Serialize(envelope, _options);
        return System.Text.Encoding.UTF8.GetBytes(json);
    }

    /// <inheritdoc/>
    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        var result = DeserializeWithPayload(data);
        if (result is null) return default;

        var (envelope, element) = result.Value;
        if (envelope is null || element.ValueKind == JsonValueKind.Undefined) return default;

        return element.Deserialize<T>(_options);
    }

    /// <inheritdoc/>
    public object? Deserialize(ReadOnlyMemory<byte> data, Type type)
    {
        var result = DeserializeWithPayload(data);
        if (result is null) return null;

        var (envelope, element) = result.Value;
        if (envelope is null || element.ValueKind == JsonValueKind.Undefined) return null;

        var rawJson = element.GetRawText();
        return JsonSerializer.Deserialize(rawJson, type, _options);
    }

    /// <summary>
    /// Deserializes the raw bytes into a <see cref="MessageEnvelope"/>.
    /// This is the first step for any message — extract the envelope,
    /// then use EventType to determine how to deserialize the Payload.
    /// </summary>
    public MessageEnvelope? DeserializeEnvelope(ReadOnlyMemory<byte> data)
    {
        var json = System.Text.Encoding.UTF8.GetString(data.Span);
        return JsonSerializer.Deserialize<MessageEnvelope>(json, _options);
    }

    /// <summary>
    /// Deserializes the envelope and returns the raw Payload as a JsonElement.
    /// The caller is responsible for further deserialization.
    /// </summary>
    /// <param name="data">The raw message bytes.</param>
    /// <returns>
    /// A tuple with the envelope metadata and the raw payload JsonElement.
    /// </returns>
    public (MessageEnvelope Envelope, JsonElement Payload)? DeserializeWithPayload(ReadOnlyMemory<byte> data)
    {
        var envelope = DeserializeEnvelope(data);
        if (envelope is null) return null;

        if (envelope.Payload is JsonElement element)
        {
            return (envelope, element);
        }

        // Payload was not a JsonElement (edge case), serialize and re-parse
        var payloadJson = JsonSerializer.Serialize(envelope.Payload, _options);
        var element2 = JsonSerializer.Deserialize<JsonElement>(payloadJson);
        return (envelope, element2);
    }
}
