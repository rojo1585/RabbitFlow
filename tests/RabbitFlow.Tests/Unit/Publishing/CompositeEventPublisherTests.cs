using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Publishing;


namespace RabbitFlow.Tests.Unit.Publishing;

public class CompositeEventPublisherTests
{
    private record TestEvent(string Data) : IIntegrationEvent;

    private  ILoggerFactory CreateLoggerFactory()
    {
        return LoggerFactory.Create(builder => builder.AddDebug());
    }

    private  RabbitMqMetrics CreateMetrics()
    {
        return new RabbitMqMetrics("Test.RabbitMQ");
    }

    [Fact]
    public void Constructor_WithValidProducers_DoesNotThrow()
    {
        var connectionRegistry = new Mock<IRabbitConnectionRegistry>();
        var serializer = new Mock<IMessageSerializer>();
        var loggerFactory = CreateLoggerFactory();
        var metrics = CreateMetrics();

        var producers = new[]
        {
        new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created"
        },
        new RabbitProducerOptions
        {
            ServiceKey = "payments",
            ConnectionName = "main",
            ExchangeName = "payments",
            RoutingKey = "payment-processed"
        }
    };

        var act = () => new CompositeEventPublisher(connectionRegistry.Object, serializer.Object, producers, loggerFactory, metrics);

        act.Should().NotThrow<RabbitMqConfigurationException>();
    }

    [Fact]
    public void Constructor_WithEmptyProducers_DoesNotThrow()
    {
       var connectionRegistry = new Mock<IRabbitConnectionRegistry>();
        var serializer = new Mock<IMessageSerializer>();
        var loggerFactory = CreateLoggerFactory();
        var metrics = CreateMetrics();

        var act = () => new CompositeEventPublisher(connectionRegistry.Object, serializer.Object,[], loggerFactory, metrics);

        act.Should().NotThrow();
    }
}
