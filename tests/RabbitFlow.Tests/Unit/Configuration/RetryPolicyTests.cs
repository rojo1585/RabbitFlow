using FluentAssertions;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;

public class RetryPolicyTests
{
    private RabbitConsumerOptions CreateConsumerOptions(int maxRetries = 3) => new()
    {
        ServiceKey = "test",
        ConnectionName = "main",
        ExchangeName = "test",
        QueueName = "test.queue",
        RoutingKey = "test",
        MaxRetries = maxRetries
    };

    [Fact]
    public void ShouldRetry_ReturnsTrue_WhenUnderMax()
    {
        var policy = new RetryPolicy(CreateConsumerOptions(maxRetries: 3));
        policy.ShouldRetry(1).Should().BeTrue();
        policy.ShouldRetry(2).Should().BeTrue();
        policy.ShouldRetry(3).Should().BeTrue();
    }

    [Fact]
    public void ShouldRetry_ReturnsFalse_WhenExceedsMax()
    {
        var policy = new RetryPolicy(CreateConsumerOptions(maxRetries: 3));
        policy.ShouldRetry(4).Should().BeFalse();
        policy.ShouldRetry(5).Should().BeFalse();
    }

    [Fact]
    public void GetRetryDelay_ReturnsCorrectDelays()
    {
        var policy = new RetryPolicy(CreateConsumerOptions());
        // deliveryCount=1 => Delays[0] = 0s
        policy.GetRetryDelay(1).Should().Be(TimeSpan.Zero);
        // deliveryCount=2 => Delays[1] = 5s
        policy.GetRetryDelay(2).Should().Be(TimeSpan.FromSeconds(5));
        // deliveryCount=3 => Delays[2] = 30s
        policy.GetRetryDelay(3).Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void GetRetryDelay_ReturnsNull_WhenOutOfRange()
    {
        var policy = new RetryPolicy(CreateConsumerOptions());
        policy.GetRetryDelay(0).Should().BeNull();
        policy.GetRetryDelay(4).Should().BeNull();
    }

    [Fact]
    public void MaxRetries_IsSetFromOptions()
    {
        var policy = new RetryPolicy(CreateConsumerOptions(maxRetries: 5));
        policy.MaxRetries.Should().Be(5);
    }
}
