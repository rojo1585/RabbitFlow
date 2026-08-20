using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions;

/// <summary>
/// Represents a message that has been routed to the dead-letter queue after
/// exhausting all configured retry attempts.
/// Passed to <see cref="IDeadLetterHandler.HandleAsync"/>.
/// </summary>
public sealed class DeadLetterMessage
{
    /// <summary>
    /// The routing key the message was originally published with.
    /// </summary>
    public required string OriginalRoutingKey { get; init; }

    /// <summary>
    /// The exchange the message was originally published to.
    /// </summary>
    public string OriginalExchange { get; init; } = string.Empty;

    /// <summary>
    /// The raw serialized body of the original message.
    /// Use <see cref="IMessageSerializer.Deserialize"/> to deserialize.
    /// </summary>
    public required ReadOnlyMemory<byte> OriginalBody { get; init; }

    /// <summary>
    /// The full name of the exception type that caused the final failure.
    /// May be null if the failure was not due to an exception in the handler.
    /// </summary>
    public string? ExceptionType { get; init; }

    /// <summary>
    /// The exception message from the final failure.
    /// May be null if the failure was not due to an exception in the handler.
    /// </summary>
    public string? ExceptionMessage { get; init; }

    /// <summary>
    /// How many times the message was attempted before being dead-lettered.
    /// Should equal <see cref="Configuration.RabbitConsumerOptions.MaxRetries"/>.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// UTC timestamp of when the message was dead-lettered.
    /// </summary>
    public DateTime DeadLetteredAt { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// The <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/>
    /// of the consumer that failed to process this message.
    /// </summary>
    public required string ConsumerKey { get; init; }

    /// <summary>
    /// The correlation ID from the original message, if present.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// The message ID from the original message, if present.
    /// </summary>
    public string? MessageId { get; init; }
}
