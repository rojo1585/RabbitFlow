using FluentAssertions;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;


public class RabbitProducerOptionsTests
{
    private RabbitProducerOptions CreateOptions() => new()
    {
        ServiceKey = "test-producer",
        ConnectionName = "main",
        ExchangeName = "test",
        RoutingKey = "test"
    };

    [Fact]
    public void DefaultExchangeType_IsDirect()
    {
        CreateOptions().ExchangeType.Should().Be("direct");
    }

    [Fact]
    public void DefaultMandatory_IsTrue()
    {
        CreateOptions().Mandatory.Should().BeTrue();
    }

    [Fact]
    public void DefaultEnablePublisherConfirms_IsTrue()
    {
        CreateOptions().EnablePublisherConfirms.Should().BeTrue();
    }

    [Fact]
    public void DefaultPublishConfirmTimeoutMs_Is5000()
    {
        CreateOptions().PublishConfirmTimeoutMs.Should().Be(5000);
    }

    [Fact]
    public void DefaultAutoDeclareTopology_IsTrue()
    {
        CreateOptions().AutoDeclareTopology.Should().BeTrue();
    }

    [Fact]
    public void DefaultChannelPoolSize_Is4()
    {
        CreateOptions().ChannelPoolSize.Should().Be(4);
    }
}
