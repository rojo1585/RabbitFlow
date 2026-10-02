namespace RedRabbit.Exceptions
{
    /// <summary>
    /// Thrown when a message cannot be deserialized (corrupt JSON, unsupported type, invalid arguments).
    /// The consumer routes this exception DIRECTLY to the DLQ (or discards if EnableDeadLetter=false)
    /// WITHOUT counting as a retry — a message that cannot be deserialized will never succeed on retry,
    /// so retrying is pointless and wastes the retry budget.
    /// </summary>
    /// <remarks>
    /// The original deserialization exception (JsonException, NotSupportedException, ArgumentException)
    /// is preserved as <see cref="Exception.InnerException"/> for diagnostic purposes.
    /// </remarks>
    /// <remarks>
    /// Creates a new MessageDeserializationException with the target type name and inner exception.
    /// </remarks>
    /// <param name="targetTypeName">The type name that was being deserialized.</param>
    /// <param name="innerException">The original deserialization exception.</param>
    public sealed class MessageDeserializationException(string targetTypeName, Exception innerException) : RabbitMqException($"Failed to deserialize message to type '{targetTypeName}'. See inner exception for details.", innerException)
    {
        /// <summary>
        /// The type name that was being deserialized when the failure occurred.
        /// </summary>
        public string TargetTypeName { get; } = targetTypeName;
    }
}
