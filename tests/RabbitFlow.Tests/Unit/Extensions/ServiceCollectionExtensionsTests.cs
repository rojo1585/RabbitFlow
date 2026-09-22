using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
using RabbitFlow.Infrastructure.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Extensions;




public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddRabbitMQ_RegistersIEventPublisher()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        services.AddLogging();
        services.AddRabbitMQ(config);

        var sp = services.BuildServiceProvider();
        var a = sp.GetService<IEventPublisher>();
        sp.GetService<IEventPublisher>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMQ_RegistersIBatchEventPublisher()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddRabbitMQ(config);

        var sp = services.BuildServiceProvider();
        sp.GetService<IBatchEventPublisher>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMQ_RegistersIMessageSerializer()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddRabbitMQ(config);

        var sp = services.BuildServiceProvider();
        sp.GetService<IMessageSerializer>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMQ_RegistersRabbitMqMetrics()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddRabbitMQ(config);

        var sp = services.BuildServiceProvider();
        sp.GetService<RabbitMqMetrics>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMQ_WithInstrumentationName_SetsMetricsName()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        const string customName = "MyCompany.RabbitMQ";

        services.AddRabbitMQ(config, configure: builder => builder.WithInstrumentationName(customName));

        var sp = services.BuildServiceProvider();
        var metrics = sp.GetRequiredService<RabbitMqMetrics>();
        metrics.MeterName.Should().Be(customName);
    }

    [Fact]
    public void AddRabbitMQ_WithCustomSerializer_RegistersSerializer()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();

        services.AddRabbitMQ(config, configure: builder => builder.WithSerializer<SystemTextJsonSerializer>());

        var sp = services.BuildServiceProvider();
        var serializer = sp.GetService<IMessageSerializer>();
        serializer.Should().NotBeNull();
        serializer.Should().BeOfType<SystemTextJsonSerializer>();
    }

    [Fact]
    public void AddRabbitMQ_WithCodeConfig_RegistersServices()
    {
        var services = new ServiceCollection();

        services.AddRabbitMQ(settings =>
        {
            settings.Connections["main"] = new RabbitConnectionOptions
            {
                Name = "main",
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
            settings.Producers.Add(new RabbitProducerOptions
            {
                ServiceKey = "orders",
                ConnectionName = "main",
                ExchangeName = "orders",
                RoutingKey = "order-created"
            });
        });

        var sp = services.BuildServiceProvider();
        sp.GetService<IEventPublisher>().Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMQ_CodeConfig_WithInstrumentationName_SetsMetricsName()
    {
        var services = new ServiceCollection();
        const string customName = "Custom.Instrumentation";

        services.AddRabbitMQ(
            settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    Name = "main",
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                };
                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "test",
                    ConnectionName = "main",
                    ExchangeName = "test",
                    RoutingKey = "test"
                });
            },
            configureBuilder: builder => builder.WithInstrumentationName(customName));

        var sp = services.BuildServiceProvider();
        var metrics = sp.GetRequiredService<RabbitMqMetrics>();
        metrics.MeterName.Should().Be(customName);
    }

    [Fact]
    public void AddRabbitMQ_Throws_WhenServicesNull()
    {
        ServiceCollection? services = null;
        var config = CreateConfiguration();

        var act = () => services!.AddRabbitMQ(config);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddRabbitMQ_Throws_WhenConfigurationNull()
    {
        var services = new ServiceCollection();
        IConfiguration? config = null;

        var act = () => services.AddRabbitMQ(config!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddRabbitMQ_Throws_WhenCalledTwice_WithConfigOverload()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        services.AddRabbitMQ(config);

        var act = () => services.AddRabbitMQ(config);

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*AddRabbitMQ has already been called*");
    }

    [Fact]
    public void AddRabbitMQ_Throws_WhenCalledTwice_WithDifferentOverloads()
    {
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        services.AddRabbitMQ(config);

        var act = () => services.AddRabbitMQ(settings =>
        {
            settings.Connections["other"] = new RabbitConnectionOptions
            {
                Name = "other",
                HostName = "other-host",
                UserName = "guest",
                Password = "guest"
            };
        });

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*AddRabbitMQ has already been called*");
    }

    [Fact]
    public void AddRabbitMQ_Throws_WhenCalledTwice_WithCodeConfigOverload()
    {
        var services = new ServiceCollection();
        services.AddRabbitMQ(settings =>
        {
            settings.Connections["main"] = new RabbitConnectionOptions
            {
                Name = "main",
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
        });

        var act = () => services.AddRabbitMQ(settings =>
        {
            settings.Connections["other"] = new RabbitConnectionOptions
            {
                Name = "other",
                HostName = "other-host",
                UserName = "guest",
                Password = "guest"
            };
        });

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*AddRabbitMQ has already been called*");
    }

    [Fact]
    public void AddRabbitMQ_ErrorMessage_GuidesToMultiConnectionPattern()
    {
        // The error message should mention the Connections dictionary so the user
        // knows the correct pattern for multi-broker support.
        var services = new ServiceCollection();
        var config = CreateConfiguration();
        services.AddRabbitMQ(config);

        try
        {
            services.AddRabbitMQ(config);
        }
        catch (InvalidOperationException ex)
        {
            ex.Message.Should().Contain("Connections dictionary");
            ex.Message.Should().Contain("single AddRabbitMQ call");
        }
    }

    [Fact]
    public void AddRabbitMQ_Succeeds_WithMultipleConnectionsInSingleCall()
    {
        // Multi-broker support is achieved within a SINGLE AddRabbitMQ call by adding
        // multiple entries to the Connections dictionary. This is the supported pattern.
        var services = new ServiceCollection();

        services.AddRabbitMQ(settings =>
        {
            // First broker
            settings.Connections["main"] = new RabbitConnectionOptions
            {
                Name = "main",
                HostName = "broker1.example.com",
                UserName = "guest",
                Password = "guest"
            };
            // Second broker 
            settings.Connections["secondary"] = new RabbitConnectionOptions
            {
                Name = "secondary",
                HostName = "broker2.example.com",
                UserName = "guest",
                Password = "guest"
            };
            // Producer on the first broker
            settings.Producers.Add(new RabbitProducerOptions
            {
                ServiceKey = "orders",
                ConnectionName = "main",
                ExchangeName = "orders",
                RoutingKey = "order-created"
            });
            // Consumer on the second broker
            settings.Consumers.Add(new RabbitConsumerOptions
            {
                ServiceKey = "notifications-consumer",
                ConnectionName = "secondary",
                ExchangeName = "notifications",
                QueueName = "notifications.pending",
                RoutingKey = "#"
            });
        });

        var sp = services.BuildServiceProvider();
        sp.GetService<IEventPublisher>().Should().NotBeNull();
    }

    private static IConfiguration CreateConfiguration()
    {
        var json = """
        {
            "RabbitMQ": {
                "Connections": {
                    "main": {
                        "Name": "main",
                        "HostName": "localhost",
                        "UserName": "guest",
                        "Password": "guest"
                    }
                },
                "Producers": [
                    {
                        "ServiceKey": "test",
                        "ConnectionName": "main",
                        "ExchangeName": "test",
                        "RoutingKey": "test"
                    }
                ]
            }
        }
        """;

        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, json);

        try
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile(tempFile, optional: false, reloadOnChange: false)
                .Build();
            return config;
        }
        finally
        {
        }
    }
}