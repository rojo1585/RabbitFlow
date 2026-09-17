using RabbitFlow.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RabbitFlow.Infrastructure.Serializartion;

/// <summary>
/// Default message serializer using System.Text.Json.
/// Wraps event payloads in a <see cref="MessageEnvelope"/> for type-safe deserialization.
/// 
/// <para>
/// <b>Performance:</b> Deserialization uses <see cref="JsonDocument"/> + <c>JsonElement.Deserialize&lt;T&gt;()</c>
/// to avoid the double-serialization anti-pattern. The payload <see cref="JsonElement"/> is
/// deserialized directly to the target type without an intermediate <c>string</c> round-trip.
/// </para>
/// 
/// <para>
/// Publish path: <c>T → MessageEnvelope → JSON → byte[]</c> (1 pass).
/// Consume path: <c>byte[] → JsonDocument → root["payload"].Deserialize&lt;T&gt;()</c> (1 pass).
/// </para>
/// </summary>
public sealed class SystemTextJsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;
    private static readonly byte[] s_payloadPropertyName = System.Text.Encoding.UTF8.GetBytes("payload");

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

        return JsonSerializer.SerializeToUtf8Bytes(envelope, _options);
    }

    /// <inheritdoc/>
    public T? Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        using var doc = JsonDocument.Parse(data);

        if (!doc.RootElement.TryGetProperty(s_payloadPropertyName, out var payloadElement))
            return default;

        return JsonSerializer.Deserialize<T>(payloadElement, _options);
    }

    /// <inheritdoc/>
    public object? Deserialize(ReadOnlyMemory<byte> data, Type type)
    {
        using var doc = JsonDocument.Parse(data);

        if (!doc.RootElement.TryGetProperty(s_payloadPropertyName, out var payloadElement))
            return null;

        return JsonSerializer.Deserialize(payloadElement, type, _options);
    }

    /// <summary>
    /// Deserializes the raw bytes into a <see cref="MessageEnvelope"/>.
    /// Uses <c>ReadOnlySpan&lt;byte&gt;</c> overload to avoid string allocation.
    /// </summary>
    public MessageEnvelope? DeserializeEnvelope(ReadOnlyMemory<byte> data)
    {
        return JsonSerializer.Deserialize<MessageEnvelope>(data.Span, _options);
    }

    /// <summary>
    /// Extracts envelope metadata AND the raw payload <see cref="JsonElement"/>
    /// using <see cref="DeserializeEnvelope"/>. The <see cref="JsonElement"/>
    /// is backed by a <see cref="JsonDocument"/> that stays alive via GC
    /// as long as the <see cref="JsonElement"/> is referenced.
    /// </summary>
    /// <param name="data">The raw message bytes.</param>
    /// <returns>
    /// A tuple with the envelope metadata and the raw payload <see cref="JsonElement"/>,
    /// or <c>null</c> if the bytes are not valid JSON or have no payload.
    /// </returns>
    public (MessageEnvelope Envelope, JsonElement Payload)? DeserializeWithPayload(ReadOnlyMemory<byte> data)
    {
        var envelope = DeserializeEnvelope(data);
        if (envelope is null) return null;

        if (envelope.Payload is JsonElement element)
            return (envelope, element);

        var payloadJson = JsonSerializer.Serialize(envelope.Payload, _options);
        var fallbackElement = JsonSerializer.Deserialize<JsonElement>(payloadJson, _options);
        return (envelope, fallbackElement);
    }
}