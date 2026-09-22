using FluentAssertions;
using RabbitFlow.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Diagnostics;


public class RabbitMqMetricsTests
{
    private const string TestMeterName = "Test.RabbitMQ.Metrics";

    [Fact]
    public void Constructor_SetsMeterName()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.MeterName.Should().Be(TestMeterName);
    }

    [Fact]
    public void Published_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.Published.Should().NotBeNull();
    }

    [Fact]
    public void PublishErrors_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.PublishErrors.Should().NotBeNull();
    }

    [Fact]
    public void Consumed_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.Consumed.Should().NotBeNull();
    }

    [Fact]
    public void ConsumeErrors_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.ConsumeErrors.Should().NotBeNull();
    }

    [Fact]
    public void Retried_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.Retried.Should().NotBeNull();
    }

    [Fact]
    public void DeadLettered_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.DeadLettered.Should().NotBeNull();
    }

    [Fact]
    public void ProcessingDurationMs_HistogramIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.ProcessingDurationMs.Should().NotBeNull();
    }

    [Fact]
    public void PublishDurationMs_HistogramIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.PublishDurationMs.Should().NotBeNull();
    }

    [Fact]
    public void BatchesDispatched_CounterIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.BatchesDispatched.Should().NotBeNull();
    }

    [Fact]
    public void BatchSize_HistogramIsNotNull()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.BatchSize.Should().NotBeNull();
    }

    [Fact]
    public void TagNameConstants_AreCorrect()
    {
        RabbitMqMetrics.TagProducerKey.Should().Be("producer_key");
        RabbitMqMetrics.TagConsumerKey.Should().Be("consumer_key");
        RabbitMqMetrics.TagEventType.Should().Be("event_type");
        RabbitMqMetrics.TagExchange.Should().Be("exchange");
        RabbitMqMetrics.TagQueue.Should().Be("queue");
        RabbitMqMetrics.TagErrorType.Should().Be("error_type");
        RabbitMqMetrics.TagAttempt.Should().Be("attempt");
        RabbitMqMetrics.TagDlqName.Should().Be("dlq_name");
        RabbitMqMetrics.TagFlushReason.Should().Be("flush_reason");
    }

    [Fact]
    public void Published_CanBeIncremented()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.Published.Add(1);
    }

    [Fact]
    public void ProcessingDurationMs_CanRecordValue()
    {
        using var metrics = new RabbitMqMetrics(TestMeterName);
        metrics.ProcessingDurationMs.Record(42.5);
    }
}
