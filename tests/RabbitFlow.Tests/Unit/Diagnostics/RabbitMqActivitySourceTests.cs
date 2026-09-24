using FluentAssertions;
using RabbitFlow.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Diagnostics;


public class RabbitMqActivitySourceTests
{
    [Fact]
    public void DefaultSourceName_IsDefaultInstrumentationName()
    {
        RabbitMqActivitySource.SourceName.Should().Be(RabbitFlow.Configuration.RabbitMqSettings.DefaultInstrumentationName);
    }

    [Fact]
    public void Source_IsNotNull()
    {
        RabbitMqActivitySource.Source.Should().NotBeNull();
    }

    [Fact]
    public void ActivityNameConstants_HaveCorrectFormat()
    {
        RabbitMqActivitySource.PublishActivityName.Should().Be("{event-type} publish");
        RabbitMqActivitySource.ReceiveActivityName.Should().Be("{event-type} receive");
        RabbitMqActivitySource.ProcessActivityName.Should().Be("{event-type} process");
    }

    [Fact]
    public void TagConstants_FollowOTelSemanticConventions()
    {
        RabbitMqActivitySource.TagMessagingSystem.Should().Be("messaging.system");
        RabbitMqActivitySource.TagMessagingDestinationKind.Should().Be("messaging.destination.kind");
        RabbitMqActivitySource.TagMessagingDestinationName.Should().Be("messaging.destination.name");
        RabbitMqActivitySource.TagMessagingOperation.Should().Be("messaging.operation");
        RabbitMqActivitySource.TagMessagingRabbitmqRoutingKey.Should().Be("messaging.rabbitmq.routing_key");
    }

    [Fact]
    public void W3CHeaders_AreCorrect()
    {
        RabbitMqActivitySource.TraceParentHeader.Should().Be("traceparent");
        RabbitMqActivitySource.TraceStateHeader.Should().Be("tracestate");
    }
}
