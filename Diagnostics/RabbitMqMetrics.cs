using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text;

namespace RabbitFlow.Diagnostics;


/// <summary>
/// Central <see cref="Meter"/> for all RabbitMQ metrics.
/// 
/// <para>
/// Uses the built-in <c>System.Diagnostics.Metrics</c> API (.NET 8+).
/// Metrics are automatically collected by any OpenTelemetry SDK configured with
/// <c>AddMeter(MeterName)</c>.
/// </para>
/// 
/// <para>
/// The <see cref="MeterName"/> is determined at construction time via the
/// <see cref="RabbitMqBuilder.WithInstrumentationName"/> builder method or
/// <see cref="Configuration.RabbitMqSettings.InstrumentationName"/> setting.
/// </para>
/// 
/// <para>
/// <b>Usage in the host:</b>
/// <code>
/// services.AddRabbitMQ(configuration);
/// services.AddOpenTelemetry()
///     .WithMetrics(metrics => metrics
///         .AddMeter(RabbitMqMetrics.MeterName));
/// </code>
/// </para>
/// </summary>
public sealed class RabbitMqMetrics
{
    /// <summary>
    /// The meter name resolved at construction time.
    /// Consumers must pass this exact string to <c>AddMeter()</c>.
    /// </summary>
    public string MeterName { get; }

    private readonly Meter _meter;

    // ────────────────────────────────────────────────
    // Counters
    // ────────────────────────────────────────────────

    /// <summary>
    /// Number of messages published. Incremented on successful publish.
    /// <para>Tags: <c>producer_key</c>, <c>event_type</c>, <c>exchange</c></para>
    /// </summary>
    public Counter<long> Published { get; }

    /// <summary>
    /// Number of publish errors (nack, timeout, channel fault).
    /// <para>Tags: <c>producer_key</c>, <c>event_type</c>, <c>error_type</c></para>
    /// </summary>
    public Counter<long> PublishErrors { get; }

    /// <summary>
    /// Number of messages successfully consumed and acknowledged.
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c></para>
    /// </summary>
    public Counter<long> Consumed { get; }

    /// <summary>
    /// Number of handler execution errors (before retry decision).
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>error_type</c>, <c>queue</c></para>
    /// </summary>
    public Counter<long> ConsumeErrors { get; }

    /// <summary>
    /// Number of messages being requeued for retry.
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c>, <c>attempt</c></para>
    /// </summary>
    public Counter<long> Retried { get; }

    /// <summary>
    /// Number of messages dead-lettered (retries exhausted).
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c>, <c>dlq_name</c></para>
    /// </summary>
    public Counter<long> DeadLettered { get; }

    // ────────────────────────────────────────────────
    // Histograms
    // ────────────────────────────────────────────────

    /// <summary>
    /// Duration of handler execution in milliseconds.
    /// Measured from handler invocation to ack (or error).
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c></para>
    /// </summary>
    public Histogram<double> ProcessingDurationMs { get; }

    /// <summary>
    /// Duration of publish operation in milliseconds.
    /// <para>Tags: <c>producer_key</c>, <c>event_type</c>, <c>exchange</c></para>
    /// </summary>
    public Histogram<double> PublishDurationMs { get; }

    // ────────────────────────────────────────────────
    // Batch Consumer Metrics
    // ────────────────────────────────────────────────

    /// <summary>
    /// Number of batches dispatched by batch consumers.
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c>, <c>flush_reason</c></para>
    /// </summary>
    public Counter<long> BatchesDispatched { get; }

    /// <summary>
    /// Size of each dispatched batch (histogram).
    /// <para>Tags: <c>consumer_key</c>, <c>event_type</c>, <c>queue</c></para>
    /// </summary>
    public Histogram<int> BatchSize { get; }

    /// <summary>
    /// Creates a new <see cref="RabbitMqMetrics"/> instance with the given meter name.
    /// Typically registered as a singleton in DI.
    /// </summary>
    /// <param name="meterName">
    /// The instrumentation name. Must match the <see cref="RabbitMqActivitySource.SourceName"/>.
    /// </param>
    public RabbitMqMetrics(string meterName)
    {
        MeterName = meterName;
        _meter = new Meter(meterName, "1.0.0");

        Published = _meter.CreateCounter<long>(
            name: "rabbitmq.published",
            unit: "{message}",
            description: "Number of messages published to RabbitMQ.");

        PublishErrors = _meter.CreateCounter<long>(
            name: "rabbitmq.publish_errors",
            unit: "{error}",
            description: "Number of publish errors (nack, timeout, channel fault).");

        Consumed = _meter.CreateCounter<long>(
            name: "rabbitmq.consumed",
            unit: "{message}",
            description: "Number of messages successfully consumed and acknowledged.");

        ConsumeErrors = _meter.CreateCounter<long>(
            name: "rabbitmq.consume_errors",
            unit: "{error}",
            description: "Number of handler execution errors during consumption.");

        Retried = _meter.CreateCounter<long>(
            name: "rabbitmq.retried",
            unit: "{message}",
            description: "Number of messages requeued for retry.");

        DeadLettered = _meter.CreateCounter<long>(
            name: "rabbitmq.dead_lettered",
            unit: "{message}",
            description: "Number of messages sent to dead-letter queue.");

        ProcessingDurationMs = _meter.CreateHistogram<double>(
            name: "rabbitmq.processing_duration_ms",
            unit: "ms",
            description: "Duration of handler execution in milliseconds.");

        PublishDurationMs = _meter.CreateHistogram<double>(
            name: "rabbitmq.publish_duration_ms",
            unit: "ms",
            description: "Duration of publish operation in milliseconds.");

        BatchesDispatched = _meter.CreateCounter<long>(
            name: "rabbitmq.batches_dispatched",
            unit: "{batch}",
            description: "Number of batches dispatched by batch consumers.");

        BatchSize = _meter.CreateHistogram<int>(
            name: "rabbitmq.batch_size",
            unit: "{message}",
            description: "Size of each dispatched batch.");
    }

    /// <summary>
    /// Disposes the underlying <see cref="Meter"/>.
    /// </summary>
    public void Dispose() => _meter.Dispose();

    // ────────────────────────────────────────────────
    // Tag names (public constants for documentation)
    // ────────────────────────────────────────────────

    /// <summary>Tag: the producer service key.</summary>
    public const string TagProducerKey = "producer_key";

    /// <summary>Tag: the consumer service key.</summary>
    public const string TagConsumerKey = "consumer_key";

    /// <summary>Tag: the full event type name.</summary>
    public const string TagEventType = "event_type";

    /// <summary>Tag: the exchange name.</summary>
    public const string TagExchange = "exchange";

    /// <summary>Tag: the queue name.</summary>
    public const string TagQueue = "queue";

    /// <summary>Tag: the exception type full name.</summary>
    public const string TagErrorType = "error_type";

    /// <summary>Tag: the retry attempt number (1-based).</summary>
    public const string TagAttempt = "attempt";

    /// <summary>Tag: the dead-letter queue name.</summary>
    public const string TagDlqName = "dlq_name";

    /// <summary>Tag: why the batch was flushed ("size" or "timeout").</summary>
    public const string TagFlushReason = "flush_reason";
}