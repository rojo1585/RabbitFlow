using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;

namespace RabbitFlow.Tests.Integration;

[Collection(RabbitMqCollection.Name)]
public class MultiConnectionAndPublisherConfirmsTests(RabbitMqFixture fixture)
{
    [EventVersion("system-event", 1)]
    public record SystemEvent(string Source, string Message) : IIntegrationEvent;

    [EventVersion("business-event", 1)]
    public record BusinessEvent(Guid Id, string Data) : IIntegrationEvent;

    private class SystemEventHandler : IRabbitHandler<SystemEvent>
    {
        public string ConsumerKey => "system-consumer";
        public static readonly List<SystemEvent> Received = [];

        public Task HandleAsync(SystemEvent @event, MessageContext context)
        {
            Received.Add(@event);
            return Task.CompletedTask;
        }
    }

    private class BusinessEventHandler : IRabbitHandler<BusinessEvent>
    {
        public string ConsumerKey => "business-consumer";
        public static readonly List<BusinessEvent> Received = [];

        public Task HandleAsync(BusinessEvent @event, MessageContext context)
        {
            Received.Add(@event);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task MultipleConnections_PublishAndConsume_BothWork()
    {
        SystemEventHandler.Received.Clear();
        BusinessEventHandler.Received.Clear();

        using var host = await BuildMultiConnectionHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var sysEvent = new SystemEvent("monitor", "health-ok");
        var bizEvent = new BusinessEvent(Guid.NewGuid(), "order-123");

        await publisher.PublishAsync("system-producer", sysEvent);
        await publisher.PublishAsync("business-producer", bizEvent);

        var sysReceived = await WaitForAsync(() => SystemEventHandler.Received.FirstOrDefault(e => e.Source == "monitor"), timeout: TimeSpan.FromSeconds(10));

        var bizReceived = await WaitForAsync(() => BusinessEventHandler.Received.FirstOrDefault(e => e.Id == bizEvent.Id), timeout: TimeSpan.FromSeconds(10));

        sysReceived.Should().NotBeNull();
        sysReceived!.Message.Should().Be("health-ok");

        bizReceived.Should().NotBeNull();
        bizReceived!.Data.Should().Be("order-123");
    }

    [Fact]
    public async Task PublisherConfirms_SuccessfulPublish_CompletesWithoutException()
    {
        using var host = await BuildSingleProducerHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new BusinessEvent(Guid.NewGuid(), "confirmed");

        var act = () => publisher.PublishAsync(@event);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task PublishBatchAsync_AllMessagesDelivered()
    {
        BusinessEventHandler.Received.Clear();

        using var host = await BuildSingleProducerHostAsync();

        var batchPublisher = host.Services.GetRequiredService<IBatchEventPublisher>();
        var events = Enumerable.Range(0, 10)
            .Select(i => new BusinessEvent(Guid.NewGuid(), $"batch-{i}"))
            .ToList();

        await batchPublisher.PublishBatchAsync(events);

        await WaitForAsync(() => BusinessEventHandler.Received.Count >= 10, timeout: TimeSpan.FromSeconds(15));

        foreach (var e in events)
        {
            BusinessEventHandler.Received.Should().Contain(r => r.Id == e.Id);
        }
    }

    private async Task<IHost> BuildMultiConnectionHostAsync()
    {
        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["system"] = new()
                {
                    Name = "system",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    InitialConnectRetryCount = 3,
                    MaxBackoffSeconds = 5
                };

                settings.Connections["business"] = new()
                {
                    Name = "business",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    InitialConnectRetryCount = 3,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new()
                {
                    ServiceKey = "system-producer",
                    ConnectionName = "system",
                    ExchangeName = "system",
                    ExchangeType = "direct",
                    RoutingKey = "system-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Producers.Add(new()
                {
                    ServiceKey = "business-producer",
                    ConnectionName = "business",
                    ExchangeName = "business",
                    ExchangeType = "direct",
                    RoutingKey = "business-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = "system-consumer",
                    ConnectionName = "system",
                    ExchangeName = "system",
                    ExchangeType = "direct",
                    QueueName = "system-queue",
                    RoutingKey = "system-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = "business-consumer",
                    ConnectionName = "business",
                    ExchangeName = "business",
                    ExchangeType = "direct",
                    QueueName = "business-queue",
                    RoutingKey = "business-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<SystemEventHandler>("system-consumer");
            services.AddRabbitHandler<BusinessEventHandler>("business-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(3000);
        return host;
    }

    private async Task<IHost> BuildSingleProducerHostAsync()
    {
        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new()
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

                settings.Producers.Add(new()
                {
                    ServiceKey = "biz-producer",
                    ConnectionName = "main",
                    ExchangeName = "biz",
                    ExchangeType = "direct",
                    RoutingKey = "business-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 4
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = "business-consumer",
                    ConnectionName = "main",
                    ExchangeName = "biz",
                    ExchangeType = "direct",
                    QueueName = "biz-queue",
                    RoutingKey = "business-event",
                    PrefetchCount = 20,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<BusinessEventHandler>("business-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    private static async Task<T> WaitForAsync<T>(Func<T?> condition, TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var poll = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var result = condition();
            if (result is not null)
                return result;
            await Task.Delay(poll);
        }

        throw new TimeoutException($"WaitForAsync timed out after {timeout.TotalSeconds}s");
    }

    private static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var poll = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(poll);
        }

        throw new TimeoutException($"WaitForAsync timed out after {timeout.TotalSeconds}s");
    }
}

