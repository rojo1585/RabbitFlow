using RabbitFlow.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RabbitFlow.Infrastructure.Serialization;

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
        return Serialize(message, typeof(T).FullName!, 1);
    }

    /// <summary>
    /// Serializes a message with explicit event type metadata, ensuring the envelope body
    /// is consistent with the AMQP headers (both use the same EventType / EventVersion).
    /// </summary>
    public ReadOnlyMemory<byte> Serialize<T>(T message, string eventTypeName, int eventVersion)
    {
        var envelope = new MessageEnvelope
        {
            EventType = eventTypeName,
            EventVersion = eventVersion,
            Payload = message,
        };

        return JsonSerializer.SerializeToUtf8Bytes(envelope, _options);
    }

    /// <inheritdoc/>
    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        try
        {
            var envelope = DeserializeEnvelope(data);
            if (envelope?.Payload is null) return default;

            if (envelope.Payload is JsonElement element)
                return element.Deserialize<T>(_options);

            return default;
        }
        catch (JsonException)
        {
            return default;
        }
    }

    /// <inheritdoc/>
    public object? Deserialize(ReadOnlyMemory<byte> data, Type type)
    {
        try
        {
            var envelope = DeserializeEnvelope(data);
            if (envelope?.Payload is null) return null;

            if (envelope.Payload is JsonElement element)
                return element.Deserialize(type, _options);

            return type.IsInstanceOfType(envelope.Payload) ? envelope.Payload : null;
        }
        catch (JsonException)
        {
            return default;
        }
    }

    /// <summary>
    /// Deserializes the raw bytes into a <see cref="MessageEnvelope"/>.
    /// This is the first step for any message — extract the envelope,
    /// then use EventType to determine how to deserialize the Payload.
    /// </summary>
    public MessageEnvelope? DeserializeEnvelope(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty) return null;
        return JsonSerializer.Deserialize<MessageEnvelope>(data.Span, _options);
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
        try
        {
            var envelope = DeserializeEnvelope(data);
            if (envelope is null) return null;

            if (envelope.Payload is JsonElement element)
                return (envelope, element);

            return null;
        }
        catch (JsonException)
        {
            return null;
        }

    }
}