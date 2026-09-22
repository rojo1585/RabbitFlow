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
