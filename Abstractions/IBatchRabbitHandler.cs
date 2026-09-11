using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions;

/// <summary>
/// Contract for handling a batch of events of the same type from a specific consumer.
/// 
/// <para>
/// Use this instead of <see cref="IRabbitHandler{TEvent}"/> when the handler can 
/// process multiple messages more efficiently as a batch (e.g. bulk database inserts, 
/// batch API calls, bulk file writes).
/// </para>
/// 
/// <para>
/// The consumer buffers incoming messages until either:
/// <list type="bullet">
///   <item>The batch size reaches <see cref="Configuration.RabbitConsumerOptions.BatchSize"/>.</item>
///   <item>The batch timeout <see cref="Configuration.RabbitConsumerOptions.BatchTimeoutMs"/> expires.</item>
/// </list>
/// </para>
/// 
/// <para>
/// Messages are ACKed only after the entire batch succeeds.
/// If the handler throws, ALL messages in the batch are NACKed (retry or dead-letter applies).
/// This is an all-or-nothing semantic — partial failure handling is the handler's responsibility.
/// </para>
/// </summary>
/// <typeparam name="TEvent">
/// The event type this handler processes.
/// </typeparam>
public interface IBatchRabbitHandler<TEvent> where TEvent : class
{
    /// <summary>
    /// Identifies which consumer configuration this handler belongs to.
    /// Must match a <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/>
    /// defined in the RabbitMQ configuration.
    /// </summary>
    string ConsumerKey { get; }

    /// <summary>
    /// Processes a batch of events delivered as a group.
    /// Called within a fresh DI scope that is disposed after this method returns.
    /// 
    /// <para>
    /// <b>Acknowledgment:</b> All messages in the batch are ACKed after this method returns 
    /// successfully. If this method throws, ALL messages are NACKed (subject to retry/DLQ rules).
    /// </para>
    /// </summary>
    /// <param name="events">
    /// The list of deserialized event instances. Guaranteed to have at least one element
    /// and at most <see cref="Configuration.RabbitConsumerOptions.BatchSize"/> elements.
    /// </param>
    /// <param name="contexts">
    /// Context for each event, in the same order as <paramref name="events"/>.
    /// Each entry contains correlation ID, retry count, headers, etc.
    /// </param>
    Task HandleBatchAsync(IReadOnlyList<TEvent> events, IReadOnlyList<MessageContext> contexts);
}
