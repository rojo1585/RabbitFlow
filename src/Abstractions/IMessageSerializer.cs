using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Pluggable serialization strategy for message bodies.
    /// The default implementation uses System.Text.Json with camelCase naming.
    /// Register a custom implementation via <c>WithSerializer&lt;T&gt;()</c> during setup.
    /// </summary>
    public interface IMessageSerializer
    {
        /// <summary>
        /// Serializes a message to bytes for publishing.
        /// </summary>
        /// <typeparam name="T">The message type.</typeparam>
        /// <param name="message">The message instance to serialize.</param>
        /// <returns>The serialized bytes.</returns>
        ReadOnlyMemory<byte> Serialize<T>(T message);

        /// <summary>
        /// Serializes a message to bytes for publishing, with explicit event type metadata.
        /// </summary>
        /// <typeparam name="T">The message type.</typeparam>
        /// <param name="message">The message instance to serialize.</param>
        /// <param name="eventTypeName">
        /// The logical event type name (from <c>[EventVersion]</c> attribute or <c>Type.FullName</c>).
        /// Written to the envelope's <c>EventType</c> field for cross-platform interoperability.
        /// </param>
        /// <param name="eventVersion">
        /// The event schema version (from <c>[EventVersion]</c> attribute, default 1).
        /// Written to the envelope's <c>EventVersion</c> field.
        /// </param>
        /// <returns>The serialized bytes.</returns>
        /// <remarks>
        /// Default implementation delegates to <see cref="Serialize{T}(T)"/> for backward
        /// compatibility with custom serializers that don't override this overload. The default
        /// <see cref="Infrastructure.Serialization.SystemTextJsonSerializer"/> overrides this to
        /// write the metadata into the envelope body, ensuring the body and AMQP headers are
        /// consistent.
        /// </remarks>
        ReadOnlyMemory<byte> Serialize<T>(T message, string eventTypeName, int eventVersion) => Serialize(message);

        /// <summary>
        /// Deserializes bytes back to a typed message.
        /// </summary>
        /// <typeparam name="T">The expected message type.</typeparam>
        /// <param name="data">The serialized bytes.</param>
        /// <returns>The deserialized instance, or null if deserialization fails.</returns>
        T? Deserialize<T>(ReadOnlyMemory<byte> data);

        /// <summary>
        /// Deserializes bytes to an object using a runtime type.
        /// Used when the type is resolved from the event type registry at runtime.
        /// </summary>
        /// <param name="data">The serialized bytes.</param>
        /// <param name="type">The runtime type to deserialize to.</param>
        /// <returns>The deserialized object, or null if deserialization fails.</returns>
        object? Deserialize(ReadOnlyMemory<byte> data, Type type);
    }
}
