using RabbitFlow.Abstractions;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.Consuming
{/// <summary>
/// Represents a single buffered message awaiting batch dispatch.
/// </summary>
/// <param name="Event">The deserialized event instance.</param>
/// <param name="Context">The message context (headers, tracing, retry count).</param>
/// <param name="DeliveryTag">AMQP delivery tag for ACK/NACK.</param>
/// <param name="Body">Original raw body for dead-lettering on failure.</param>
/// <param name="Properties">Original AMQP properties for dead-lettering.</param>
/// <param name="EventTypeName">Event type name for logging/metrics.</param>
    internal sealed record BufferedMessage(object Event, MessageContext Context, ulong DeliveryTag, ReadOnlyMemory<byte> Body, IReadOnlyBasicProperties Properties, string EventTypeName);

}
