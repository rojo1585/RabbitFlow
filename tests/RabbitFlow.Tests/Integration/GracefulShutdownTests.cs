using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;

namespace RabbitFlow.Tests.Integration;


[Collection(RabbitMqCollection.Name)]
public class GracefulShutdownTests(RabbitMqFixture fixture)
{
    [EventVersion("shutdown-event", 1)]
    public record ShutdownEvent(Guid Id) : IIntegrationEvent;
    private class SlowHandler : IRabbitHandler<ShutdownEvent>
    {
        public string ConsumerKey => "shutdown-consumer";
        public static int ProcessedCount;
        public static readonly List<Guid> ProcessedIds = [];

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            // Simulate work
            await Task.Delay(500, cancellationToken);
            Interlocked.Increment(ref ProcessedCount);
            ProcessedIds.Add(@event.Id);
        }
    }

    [Fact]
    public async Task StopAsync_CompletesWithoutHanging()
    {
        SlowHandler.ProcessedCount = 0;
        SlowHandler.ProcessedIds.Clear();

        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    InitialConnectRetryCount = 3,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown",
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown",
                    ExchangeType = "direct",
                    QueueName = "shutdown-queue",
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<SlowHandler>("shutdown-consumer");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(new ShutdownEvent(Guid.NewGuid()));

        await Task.Delay(2000);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync(TimeSpan.FromSeconds(30));
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task StopAsync_InFlightHandlers_CompleteBeforeShutdown()
    {
        SlowHandler.ProcessedCount = 0;
        SlowHandler.ProcessedIds.Clear();

        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    InitialConnectRetryCount = 3,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown2",
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown2",
                    ExchangeType = "direct",
                    QueueName = "shutdown2-queue",
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<SlowHandler>("shutdown-consumer");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventId = Guid.NewGuid();
        await publisher.PublishAsync(new ShutdownEvent(eventId));

        await Task.Delay(300);

        await host.StopAsync(TimeSpan.FromSeconds(30));

        SlowHandler.ProcessedIds.Should().Contain(eventId);
    }
}

