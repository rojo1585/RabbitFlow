using FluentAssertions;
using RabbitFlow.Configuration;

namespace RabbitFlow.Tests.Unit.Configuration;


public class RabbitConsumerOptionsTests
{
    private RabbitConsumerOptions CreateOptions() => new()
    {
        ServiceKey = "test-consumer",
        ConnectionName = "main",
        ExchangeName = "test",
        QueueName = "test.queue",
        RoutingKey = "test"
    };

    [Fact]
    public void DefaultPrefetchCount_Is10()
    {
        CreateOptions().PrefetchCount.Should().Be(10);
    }

    [Fact]
    public void DefaultEnableRetry_IsTrue()
    {
        CreateOptions().EnableRetry.Should().BeTrue();
    }

    [Fact]
    public void DefaultMaxRetries_Is3()
    {
        CreateOptions().MaxRetries.Should().Be(3);
    }

    [Fact]
    public void DefaultEnableDeadLetter_IsTrue()
    {
        CreateOptions().EnableDeadLetter.Should().BeTrue();
    }

    [Fact]
    public void DefaultEnableBatchConsumer_IsFalse()
    {
        CreateOptions().EnableBatchConsumer.Should().BeFalse();
    }

    [Fact]
    public void DefaultBatchSize_Is10()
    {
        CreateOptions().BatchSize.Should().Be(10);
    }

    [Fact]
    public void DefaultBatchTimeoutMs_Is5000()
    {
        CreateOptions().BatchTimeoutMs.Should().Be(5000);
    }

    [Fact]
    public void ResolvedDlxName_UsesConvention_WhenNotSet()
    {
        CreateOptions().ResolvedDlxName.Should().Be("test.queue.dlx");
    }

    [Fact]
    public void ResolvedDlqName_UsesConvention_WhenNotSet()
    {
        CreateOptions().ResolvedDlqName.Should().Be("test.queue.dlq");
    }

    [Fact]
    public void ResolvedDlxName_UsesExplicit_WhenSet()
    {
        var opts = CreateOptions() with { DeadLetterExchangeName = "custom.dlx" };
        opts.ResolvedDlxName.Should().Be("custom.dlx");
    }

    [Fact]
    public void ResolvedDlqName_UsesExplicit_WhenSet()
    {
        var opts = CreateOptions() with { DeadLetterQueueName = "custom.dlq" };
        opts.ResolvedDlqName.Should().Be("custom.dlq");
    }

    [Fact]
    public void ResolvedRetryDelays_DefaultsToZero5s30s()
    {
        var delays = CreateOptions().ResolvedRetryDelays;
        delays.Should().HaveCount(3);
        delays[0].Should().Be(TimeSpan.Zero);
        delays[1].Should().Be(TimeSpan.FromSeconds(5));
        delays[2].Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void ResolvedRetryDelays_UsesExplicit_WhenSet()
    {
        var custom = new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10) };
        var opts = CreateOptions() with { RetryDelays = custom };
        opts.ResolvedRetryDelays.Should().BeEquivalentTo(custom);
    }
}
