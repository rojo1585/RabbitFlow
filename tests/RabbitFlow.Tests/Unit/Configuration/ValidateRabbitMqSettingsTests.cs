using FluentAssertions;
using Microsoft.Extensions.Options;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;


public class ValidateRabbitMqSettingsTests
{
    private readonly ValidateRabbitMqSettings _validator = new();
    private static RabbitMqSettings ValidSettings() => new()
    {
        Connections = new Dictionary<string, RabbitConnectionOptions>
        {
            ["main"] = new()
            {
                Name = "main",
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            }
        },
        Producers =
        [
            new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created"
        }
        ],
        Consumers =
        [
            new RabbitConsumerOptions
        {
            ServiceKey = "orders-consumer",
            ConnectionName = "main",
            ExchangeName = "orders",
            QueueName = "orders-queue",
            RoutingKey = "order-created"
        }
        ]
    };

    [Fact]
    public void Validate_WithValidSettings_ReturnsSuccess()
    {
        var result = _validator.Validate(Options.DefaultName, ValidSettings());
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithEmptyConfig_ReturnsSuccess()
    {
        var result = _validator.Validate(Options.DefaultName, new RabbitMqSettings());
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithNamedOptions_SkipsValidation()
    {
        var result = _validator.Validate("custom-name", ValidSettings());
        result.Skipped.Should().BeTrue();
    }

    [Fact]
    public void Validate_NoConnectionsButProducersExist_Fails()
    {
        var settings = new RabbitMqSettings
        {
            Producers =
            [
                new RabbitProducerOptions
            {
                ServiceKey = "orders",
                ConnectionName = "missing",
                ExchangeName = "orders",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("no connections");
    }

    [Fact]
    public void Validate_NoConnectionsButConsumersExist_Fails()
    {
        var settings = new RabbitMqSettings
        {
            Consumers =
            [
                new RabbitConsumerOptions
            {
                ServiceKey = "c",
                ConnectionName = "missing",
                ExchangeName = "ex",
                QueueName = "q",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("no connections");
    }

    [Fact]
    public void Validate_DuplicateConnectionNames_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main-dup"] = settings.Connections["main"];

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_DuplicateProducerServiceKeys_Fails()
    {
        var settings = ValidSettings();
        settings.Producers.Add(new RabbitProducerOptions
        {
            ServiceKey = "orders", // dp
            ConnectionName = "main",
            ExchangeName = "orders-2",
            RoutingKey = "order-updated"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate producer ServiceKey");
        result.FailureMessage.Should().Contain("orders");
    }

    [Fact]
    public void Validate_ProducerMissingConnection_Fails()
    {
        var settings = ValidSettings();
        settings.Producers.Add(new RabbitProducerOptions
        {
            ServiceKey = "payments",
            ConnectionName = "nonexistent",
            ExchangeName = "payments",
            RoutingKey = "pay"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("nonexistent");
    }

    [Fact]
    public void Validate_DuplicateConsumerServiceKeys_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers.Add(new RabbitConsumerOptions
        {
            ServiceKey = "orders-consumer", //dp
            ConnectionName = "main",
            ExchangeName = "orders",
            QueueName = "orders-queue-2",
            RoutingKey = "order-created"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate consumer ServiceKey");
        result.FailureMessage.Should().Contain("orders-consumer");
    }

    [Fact]
    public void Validate_ConsumerMissingConnection_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers.Add(new RabbitConsumerOptions
        {
            ServiceKey = "audit-consumer",
            ConnectionName = "nonexistent",
            ExchangeName = "audit",
            QueueName = "audit-queue",
            RoutingKey = "audit"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("nonexistent");
    }


    [Fact]
    public void Validate_MultipleErrors_ReportsAll()
    {
        var settings = new RabbitMqSettings
        {
            Connections = new Dictionary<string, RabbitConnectionOptions>
            {
                ["main"] = new()
                {
                    Name = "main",
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                }
            },
            Producers =
            [
                new RabbitProducerOptions
            {
                ServiceKey = "dup",
                ConnectionName = "missing1",
                ExchangeName = "ex1",
                RoutingKey = "rk"
            },
            new RabbitProducerOptions
            {
                ServiceKey = "dup", // dp k
                ConnectionName = "missing2",
                ExchangeName = "ex2",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate producer ServiceKey");
        result.FailureMessage.Should().Contain("missing1");
        result.FailureMessage.Should().Contain("missing2");
    }
}
