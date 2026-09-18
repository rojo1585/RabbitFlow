using FluentAssertions;
using RabbitFlow.Abstractions;

namespace RabbitFlow.Tests.Unit.Abstractions;

public class MessageEnvelopeTests
{
    [Fact]
    public void EventType_IsRequired()
    {
        var envelope = new MessageEnvelope
        {
            EventType = "TestEvent",
            Payload = new { Name = "test" }
        };
        envelope.EventType.Should().Be("TestEvent");
    }

    [Fact]
    public void EventVersion_DefaultsTo1()
    {
        var envelope = new MessageEnvelope
        {
            EventType = "TestEvent",
            Payload = null
        };
        envelope.EventVersion.Should().Be(1);
    }

    [Fact]
    public void CorrelationId_CanBeSet()
    {
        var id = Guid.NewGuid().ToString();
        var envelope = new MessageEnvelope
        {
            EventType = "TestEvent",
            Payload = null,
            CorrelationId = id
        };
        envelope.CorrelationId.Should().Be(id);
    }
}

public class MessageContextTests
{
    [Fact]
    public void RetryCount_DefaultsTo0()
    {
        var ctx = new MessageContext();
        ctx.RetryCount.Should().Be(0);
    }

    [Fact]
    public void RoutingKey_DefaultsToEmpty()
    {
        var ctx = new MessageContext();
        ctx.RoutingKey.Should().BeEmpty();
    }

    [Fact]
    public void Headers_DefaultsToEmptyDictionary()
    {
        var ctx = new MessageContext();
        ctx.Headers.Should().BeEmpty();
    }

    [Fact]
    public void CanSetAllProperties()
    {
        var ctx = new MessageContext
        {
            CorrelationId = "corr-123",
            MessageId = "msg-456",
            PublishedAt = DateTime.UtcNow,
            PublisherName = "order-service",
            RetryCount = 2,
            ConsumerTag = "ctag",
            RoutingKey = "order-created",
            Exchange = "orders",
            DeliveryTag = 42,
            Redelivered = true,
            Headers = new Dictionary<string, string> { { "key", "value" } }
        };

        ctx.CorrelationId.Should().Be("corr-123");
        ctx.MessageId.Should().Be("msg-456");
        ctx.RetryCount.Should().Be(2);
        ctx.DeliveryTag.Should().Be(42);
        ctx.Redelivered.Should().BeTrue();
        ctx.Headers.Should().ContainKey("key");
    }
}

public class DeadLetterMessageTests
{
    [Fact]
    public void CanCreate_WithRequiredProperties()
    {
        var msg = new DeadLetterMessage
        {
            OriginalRoutingKey = "order-created",
            OriginalBody = System.Text.Encoding.UTF8.GetBytes("{}"),
            ConsumerKey = "orders-consumer",
            RetryCount = 3,
            ExceptionType = typeof(InvalidOperationException).FullName,
            ExceptionMessage = "Something went wrong"
        };

        msg.OriginalRoutingKey.Should().Be("order-created");
        msg.ConsumerKey.Should().Be("orders-consumer");
        msg.RetryCount.Should().Be(3);
        msg.ExceptionType.Should().NotBeNull();
    }

    [Fact]
    public void DeadLetteredAt_DefaultsToUtcNow()
    {
        var before = DateTime.UtcNow;
        var msg = new DeadLetterMessage
        {
            OriginalRoutingKey = "test",
            OriginalBody = new(),
            ConsumerKey = "test"
        };
        var after = DateTime.UtcNow;

        msg.DeadLetteredAt.Should().BeOnOrAfter(before);
        msg.DeadLetteredAt.Should().BeOnOrBefore(after);
    }
}
