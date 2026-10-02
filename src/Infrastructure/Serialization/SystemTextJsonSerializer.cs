using Microsoft.Extensions.Logging;
using RabbitFlow.Exceptions;
using RedRabbit.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedRabbit.Infrastructure.Serialization;


/// <summary>
/// Default message serializer using System.Text.Json.
/// Wraps event payloads in a <see cref="MessageEnvelope"/> for type-safe deserialization.
/// </summary>
public sealed class SystemTextJsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;
    private readonly ILogger<SystemTextJsonSerializer>? _logger;

    /// <summary>
    /// Creates a new serializer with default options (camelCase, ignore null values).
    /// </summary>
    public SystemTextJsonSerializer() : this(new JsonSerializerOptions(), loggerFactory: null) { }

    /// <summary>
    /// Creates a new serializer with default options (camelCase, ignore null values)
    /// and an optional logger factory for diagnostics.
    /// </summary>
    /// <param name="loggerFactory">
    /// Optional logger factory used to log deserialization failures.
    /// Pass <c>null</c> (or use the parameterless constructor) for a silent, no-logging instance.
    /// </param>
    public SystemTextJsonSerializer(ILoggerFactory? loggerFactory) : this(new JsonSerializerOptions(), loggerFactory) { }

    /// <summary>
    /// Creates a new serializer with custom options.
    /// </summary>
    /// <param name="options">JSON serializer options. PropertyNamingPolicy and
    /// DefaultIgnoreCondition can be customized.</param>
    /// <remarks>
    /// The supplied <paramref name="options"/> instance is <b>not</b> mutated. A shallow copy is
    /// created via the <see cref="JsonSerializerOptions"/> copy constructor and the defaults
    /// (camelCase naming, ignore null values on write) are applied only to the copy. This avoids
    /// <see cref="InvalidOperationException"/> when the caller reuses an options instance that
    /// has already been used for serialization (the options are effectively immutable after first use).
    /// </remarks>
    public SystemTextJsonSerializer(JsonSerializerOptions options) : this(options, loggerFactory: null) { }

    /// <summary>
    /// Creates a new serializer with custom options and an optional logger factory for diagnostics.
    /// </summary>
    /// <param name="options">JSON serializer options. PropertyNamingPolicy and
    /// DefaultIgnoreCondition can be customized. The instance is cloned, not mutated.</param>
    /// <param name="loggerFactory">
    /// Optional logger factory used to log deserialization failures.
    /// Pass <c>null</c> for a silent, no-logging instance.
    /// </param>
    public SystemTextJsonSerializer(JsonSerializerOptions options, ILoggerFactory? loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = new JsonSerializerOptions(options)
        {
            PropertyNamingPolicy = options.PropertyNamingPolicy ?? JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = options.DefaultIgnoreCondition == default ? JsonIgnoreCondition.WhenWritingNull : options.DefaultIgnoreCondition,
        };
        _logger = loggerFactory?.CreateLogger<SystemTextJsonSerializer>();
    }

    /// <inheritdoc/>
    public string ContentType => "application/json";

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
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            _logger?.LogWarning(ex, "Failed to deserialize message to type {Type}", typeof(T).Name);
            throw new MessageDeserializationException(typeof(T).Name, ex);
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
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            _logger?.LogWarning(ex, "Failed to deserialize message to type {Type}", type.Name);
            throw new MessageDeserializationException(type.Name, ex);
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
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            _logger?.LogWarning(ex, "Failed to deserialize message envelope with payload");
            throw new MessageDeserializationException(typeof(MessageEnvelope).Name, ex);
        }

    }
}

