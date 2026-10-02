using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitFlow.Configuration;
using RabbitFlow.Infrastructure.Publishing;
using RedRabbit.Abstractions;
using RedRabbit.Diagnostics;
using RedRabbit.Exceptions;
using RedRabbit.Infrastructure.Connection;


namespace RabbitFlow.Tests.Unit.Publishing;

public class CompositeEventPublisherTests
{
    private record TestEvent(string Data) : IIntegrationEvent;

    private ILoggerFactory CreateLoggerFactory()
    {
        return LoggerFactory.Create(builder => builder.AddDebug());
    }

    private RabbitMqMetrics CreateMetrics()
    {
        return new RabbitMqMetrics("Test.RabbitMQ");
    }

    /// <summary>
    /// Creates a real RabbitConnectionRegistry with a "main" connection registered
    /// (no broker is needed — the connection is never started, only resolved by name).
    /// The tests verify that the CompositeEventPublisher constructor doesn't throw
    /// for valid/empty producer lists — they don't actually publish.
    /// </summary>
    private RabbitConnectionRegistry CreateConnectionRegistry()
    {
        var settings = new RabbitMqSettings();
        settings.Connections["main"] = new RabbitConnectionOptions
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        var options = Options.Create(settings);
        var loggerFactory = CreateLoggerFactory();
        return new RabbitConnectionRegistry(options, loggerFactory);
    }

    [Fact]
    public void Constructor_WithValidProducers_DoesNotThrow()
    {
        var connectionRegistry = CreateConnectionRegistry();
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

        var act = () => new CompositeEventPublisher(connectionRegistry, serializer.Object, producers, loggerFactory, metrics);

        act.Should().NotThrow<RabbitMqConfigurationException>();
    }

    [Fact]
    public void Constructor_WithEmptyProducers_DoesNotThrow()
    {
        var connectionRegistry = CreateConnectionRegistry();
        var serializer = new Mock<IMessageSerializer>();
        var loggerFactory = CreateLoggerFactory();
        var metrics = CreateMetrics();

        var act = () => new CompositeEventPublisher(connectionRegistry, serializer.Object, [], loggerFactory, metrics);

        act.Should().NotThrow();
    }
}

