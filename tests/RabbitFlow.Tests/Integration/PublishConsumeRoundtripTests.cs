using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
namespace RabbitFlow.Tests.Integration;

[Collection(RabbitMqCollection.Name)]
public class PublishConsumeRoundtripTests(RabbitMqFixture fixture)
{
    [EventVersion("order-created", 1)]
    public record OrderCreatedEvent(Guid OrderId, string CustomerEmail) : IIntegrationEvent;

    [EventVersion("payment-processed", 1)]
    public record PaymentProcessedEvent(Guid PaymentId, decimal Amount) : IIntegrationEvent;

    private class OrderCreatedHandler : IRabbitHandler<OrderCreatedEvent>
    {
        public string ConsumerKey => "orders-consumer";
        public static readonly List<OrderCreatedEvent> Received = [];

        public Task HandleAsync(OrderCreatedEvent @event, MessageContext context)
        {
            Received.Add(@event);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task PublishAsync_MessageIsConsumedByHandler()
    {

        OrderCreatedHandler.Received.Clear();
        var eventName = typeof(OrderCreatedEvent).FullName!;

        using var host = await BuildHostAsync(services =>
            services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer"));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new OrderCreatedEvent(Guid.NewGuid(), "test@example.com");

        await publisher.PublishAsync(@event);

        var received = await WaitForAsync(
            () => OrderCreatedHandler.Received.FirstOrDefault(e => e.OrderId == @event.OrderId),
            timeout: TimeSpan.FromSeconds(10));

        received.Should().NotBeNull();
        received!.OrderId.Should().Be(@event.OrderId);
        received.CustomerEmail.Should().Be(@event.CustomerEmail);
    }

    [Fact]
    public async Task PublishAsync_MultipleMessages_AllAreConsumed()
    {
        OrderCreatedHandler.Received.Clear();

        using var host = await BuildHostAsync(services =>
            services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer"));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var events = Enumerable.Range(0, 5)
            .Select(i => new OrderCreatedEvent(Guid.NewGuid(), $"customer{i}@test.com"))
            .ToList();

        foreach (var e in events)
            await publisher.PublishAsync(e);


        await WaitForAsync(() => OrderCreatedHandler.Received.Count >= 5, timeout: TimeSpan.FromSeconds(15));

        foreach (var e in events)
        {
            OrderCreatedHandler.Received.Should().Contain(r => r.OrderId == e.OrderId);
        }
    }

    [Fact]
    public async Task PublishAsync_WithRoutingKeyOverride_DeliversToCorrectQueue()
    {
        OrderCreatedHandler.Received.Clear();

        using var host = await BuildHostAsync(services =>
            services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer"));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new OrderCreatedEvent(Guid.NewGuid(), "routed@test.com");

        await publisher.PublishAsync(@event, routingKey: "order-created");

        var received = await WaitForAsync(
            () => OrderCreatedHandler.Received.FirstOrDefault(e => e.OrderId == @event.OrderId),
            timeout: TimeSpan.FromSeconds(10));

        received.Should().NotBeNull();
    }

    private async Task<IHost> BuildHostAsync(Action<IServiceCollection>? configureHandlers = null)
    {
        var hostBuilder = Host.CreateDefaultBuilder();

        hostBuilder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddFilter("RabbitFlow", LogLevel.Debug);
            logging.AddConsole();
            logging.SetMinimumLevel(LogLevel.Debug);
        });

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
                    ServiceKey = "orders-producer",
                    ConnectionName = "main",
                    ExchangeName = "orders",
                    ExchangeType = "direct",
                    RoutingKey = "order-created",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = "orders-consumer",
                    ConnectionName = "main",
                    ExchangeName = "orders",
                    ExchangeType = "direct",
                    QueueName = "orders-queue",
                    RoutingKey = "order-created",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            configureHandlers?.Invoke(services);
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

        throw new TimeoutException($"WaitForAsync timed out after {timeout.TotalSeconds}s waiting for {typeof(T).Name}");
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
