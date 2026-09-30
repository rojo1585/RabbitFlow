using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
using RabbitMQ.Client;
namespace RabbitFlow.Tests.Integration;

[Collection(RabbitMqCollection.Name)]
public class RetryAndDeadLetterTests(RabbitMqFixture fixture)
{
    [EventVersion("flaky-event", 1)]
    public record FlakyEvent(Guid Id, bool ShouldSucceed) : IIntegrationEvent;

    private class FlakyHandler : IRabbitHandler<FlakyEvent>
    {
        public string ConsumerKey => "flaky-consumer";
        public static int AttemptCount;
        public static readonly List<FlakyEvent> SucceededEvents = [];
        public static bool NextAttemptSucceeds;

        public Task HandleAsync(FlakyEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AttemptCount);

            if (NextAttemptSucceeds || @event.ShouldSucceed)
            {
                SucceededEvents.Add(@event);
                return Task.CompletedTask;
            }

            throw new InvalidOperationException("Simulated transient failure");
        }
    }

    [Fact]
    public async Task HandlerFails_MessageIsRetried()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        await publisher.PublishAsync(@event);

        await Task.Delay(3000);

        FlakyHandler.AttemptCount.Should().BeGreaterOrEqualTo(1);
    }

    [Fact]
    public async Task HandlerFailsThenSucceeds_MessageIsProcessed()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: true);

        await publisher.PublishAsync(@event);

        var received = await WaitForAsync(() => FlakyHandler.SucceededEvents.FirstOrDefault(e => e.Id == @event.Id), timeout: TimeSpan.FromSeconds(10));

        received.Should().NotBeNull();
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageIsDeadLettered()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        await publisher.PublishAsync(@event);

        await Task.Delay(15000);

        FlakyHandler.AttemptCount.Should().BeGreaterOrEqualTo(1);
        FlakyHandler.SucceededEvents.Should().NotContain(e => e.Id == @event.Id);
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageReachesDlq_AfterExactly_MaxRetries_3()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        const int maxRetries = 3;
        var retryDelays = new[] { TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) };
        const string exchangeName = "retry-test-3";
        const string queueName = "retry-test-3-queue";

        using var host = await BuildHostAsync(maxRetries, retryDelays, exchangeName, queueName);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");


        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageReachesDlq_AfterExactly_MaxRetries_5()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        const int maxRetries = 5;
        var retryDelays = new[]
        {
            TimeSpan.Zero,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1)
        };
        const string exchangeName = "retry-test-5";
        const string queueName = "retry-test-5-queue";

        using var host = await BuildHostAsync(maxRetries, retryDelays, exchangeName, queueName);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);
       
        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");

        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    private async Task<IHost> BuildHostAsync()
    {
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
                    ServiceKey = "flaky-producer",
                    ConnectionName = "main",
                    ExchangeName = "flaky",
                    ExchangeType = "direct",
                    RoutingKey = "flaky-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "flaky-consumer",
                    ConnectionName = "main",
                    ExchangeName = "flaky",
                    ExchangeType = "direct",
                    QueueName = "flaky-queue",
                    RoutingKey = "flaky-event",
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    RetryDelays = [TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)]
                });
            });

            services.AddRabbitHandler<FlakyHandler>("flaky-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    /// <summary>
    /// Builds a host with parameterized retry settings and unique exchange/queue names
    /// to avoid cross-test message interference.
    /// </summary>
    private async Task<IHost> BuildHostAsync(int maxRetries, TimeSpan[] retryDelays, string exchangeName, string queueName)
    {
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
                    ServiceKey = "flaky-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    RoutingKey = "flaky-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "flaky-consumer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    QueueName = queueName,
                    RoutingKey = "flaky-event",
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });
            });

            services.AddRabbitHandler<FlakyHandler>("flaky-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    /// <summary>
    /// Polls the DLQ ({queueName}.dlq) for a single message within the given timeout.
    /// Returns null if no message arrives before the deadline.
    /// </summary>
    private async Task<BasicGetResult?> GetMessageFromDlqAsync(string queueName, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        await using var connection = await fixture.CreateRawConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        while (DateTime.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(queueName + ".dlq", autoAck: true);
            if (result is not null)
                return result;
            await Task.Delay(200);
        }
        return null;
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