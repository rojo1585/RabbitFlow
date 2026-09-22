using FluentAssertions;
using RabbitFlow.Configuration;

namespace RabbitFlow.Tests.Unit.Configuration;


public class RabbitMqSettingsTests
{
    [Fact]
    public void DefaultInstrumentationName_IsRabbitMQ()
    {
        RabbitMqSettings.DefaultInstrumentationName.Should().Be("RabbitMQ");
    }

    [Fact]
    public void SectionName_IsRabbitMQ()
    {
        RabbitMqSettings.SectionName.Should().Be("RabbitMQ");
    }

    [Fact]
    public void DefaultSettings_HaveEmptyConnections()
    {
        var settings = new RabbitMqSettings();
        settings.Connections.Should().BeEmpty();
    }

    [Fact]
    public void DefaultSettings_HaveEmptyProducers()
    {
        var settings = new RabbitMqSettings();
        settings.Producers.Should().BeEmpty();
    }

    [Fact]
    public void DefaultSettings_HaveEmptyConsumers()
    {
        var settings = new RabbitMqSettings();
        settings.Consumers.Should().BeEmpty();
    }

    [Fact]
    public void InstrumentationName_DefaultsToNull()
    {
        var settings = new RabbitMqSettings();
        settings.InstrumentationName.Should().BeNull();
    }

    [Fact]
    public void InstrumentationName_CanBeSet()
    {
        var settings = new RabbitMqSettings { InstrumentationName = "Some.RabbitMQ" };
        settings.InstrumentationName.Should().Be("Some.RabbitMQ");
    }
}
