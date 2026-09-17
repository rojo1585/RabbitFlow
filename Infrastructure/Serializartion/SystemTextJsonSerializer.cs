using RabbitFlow.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RabbitFlow.Infrastructure.Serializartion;

/// <summary>
/// Default message serializer using System.Text.Json.
/// Wraps event payloads in a <see cref="MessageEnvelope"/> for type-safe deserialization.
/// </summary>
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
        var envelope = DeserializeEnvelope(data);
        if (envelope?.Payload is null) return default;

        // Payload comes back as JsonElement (System.Text.Json doesn't know the type)
        var payloadJson = JsonSerializer.Serialize(envelope.Payload, _options);
        return JsonSerializer.Deserialize<T>(payloadJson, _options);
    }

    /// <inheritdoc/>
    public object? Deserialize(ReadOnlyMemory<byte> data, Type type)
    {
        var envelope = DeserializeEnvelope(data);
        if (envelope?.Payload is null) return null;

        var payloadJson = JsonSerializer.Serialize(envelope.Payload, _options);
        return JsonSerializer.Deserialize(payloadJson, type, _options);
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
            return (envelope, element);

        var payloadJson = JsonSerializer.Serialize(envelope.Payload, _options);
        var element2 = JsonSerializer.Deserialize<JsonElement>(payloadJson);
        return (envelope, element2);
    }
}