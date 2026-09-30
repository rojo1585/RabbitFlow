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

    [EventVersion("retry-isolation-event", 1)]
    public record RetryIsolationEvent(Guid Id) : IIntegrationEvent;

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

    /// <summary>
    /// Handler bound to consumer "retry-isolation-a" on the shared fanout exchange.
    /// ALWAYS throws — used to drive retries on consumer A so we can verify whether
    /// those retries leak to consumer B (regression guard for retry isolation).
    /// </summary>
    private class FailingHandlerA : IRabbitHandler<RetryIsolationEvent>
    {
        public string ConsumerKey => "retry-isolation-a";
        public static int AttemptCount;

        public Task HandleAsync(RetryIsolationEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AttemptCount);
            throw new InvalidOperationException("Always fails");
        }
    }

    /// <summary>
    /// Handler bound to consumer "retry-isolation-b" on the shared fanout exchange.
    /// ALWAYS succeeds and records every message it receives. If retry isolation is
    /// broken (retry queue dead-lettering to the business exchange instead of the
    /// default exchange), this handler will receive A's retries and <see cref="ReceivedCount"/>
    /// will exceed 1.
    /// </summary>
    private class SucceedingHandlerB : IRabbitHandler<RetryIsolationEvent>
    {
        public string ConsumerKey => "retry-isolation-b";
        public static int ReceivedCount;
        public static readonly List<Guid> ReceivedIds = [];

        public Task HandleAsync(RetryIsolationEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref ReceivedCount);
            ReceivedIds.Add(@event.Id);
            return Task.CompletedTask;
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
        // Arrange
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

        // Act
        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        // Assert: handler invoked exactly MaxRetries times (not more — not a zombie)
        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");

        // Assert: the message reached the DLQ
        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageReachesDlq_AfterExactly_MaxRetries_5()
    {
        // Arrange
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

        // Act
        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        // Assert: handler invoked exactly MaxRetries times (not more — not a zombie)
        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");

        // Assert: the message reached the DLQ
        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    /// <summary>
    /// Regression test for retry isolation: when consumer A fails and retries on a fanout exchange,
    /// consumer B (bound to the same exchange, different queue) must receive the ORIGINAL message
    /// exactly ONCE — it must NOT receive A's retries.
    ///
    /// <para>
    /// Before the retry-isolation fix, the retry queue's dead-letter destination was configured to use
    /// the business exchange with the original routing key. On fanout (and topic) exchanges, this
    /// caused A's retries to be broadcast to ALL queues bound to the exchange — including B's queue.
    /// With the fix, the retry queue dead-letters to the default exchange ("") with the main queue
    /// name as routing key, so retries are delivered ONLY to A's main queue.
    /// </para>
    /// <para>
    /// If this test fails with <c>SucceedingHandlerB.ReceivedCount == 3</c> (or any value greater
    /// than 1), the retry isolation fix has been reverted or broken.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Retry_DoesNotBroadcast_ToOtherConsumers_OnSameExchange()
    {
        // Arrange — reset static state on both handlers
        FailingHandlerA.AttemptCount = 0;
        SucceedingHandlerB.ReceivedCount = 0;
        SucceedingHandlerB.ReceivedIds.Clear();

        const string exchangeName = "retry-isolation-test";
        const string queueA = "retry-isolation-a-queue";
        const string queueB = "retry-isolation-b-queue";
        // fanout ignores the routing key for delivery, but RabbitFlow validation requires
        // a non-empty routing key — use a placeholder (same value for both queues is fine,
        // fanout does not filter on it).
        const string routingKey = "isolation";
        const int maxRetries = 3;
        var retryDelays = new[] { TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) };

        // Build host with TWO consumers on the SAME fanout exchange, each on its own queue.
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
                    ServiceKey = "isolation-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout", // KEY: fanout broadcasts to all bound queues
                    RoutingKey = routingKey,
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                // Consumer A — always fails, with retry
                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "retry-isolation-a",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout",
                    QueueName = queueA,
                    RoutingKey = routingKey,
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });

                // Consumer B — always succeeds, with retry (should never need it)
                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "retry-isolation-b",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout",
                    QueueName = queueB,
                    RoutingKey = routingKey,
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });
            });

            services.AddRabbitHandler<FailingHandlerA>("retry-isolation-a");
            services.AddRabbitHandler<SucceedingHandlerB>("retry-isolation-b");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        // Act — publish a single message to the fanout exchange.
        // The broker delivers one copy to queueA (A will fail and retry) and one copy to queueB 
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventId = Guid.NewGuid();
        await publisher.PublishAsync(new RetryIsolationEvent(eventId));

        // Wait for handler A to be invoked MaxRetries times (then DLQ).
        await WaitForAsync(() => FailingHandlerA.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Grace window for any spurious deliveries to B to arrive (they should not — if they
        // do, the retry queue is dead-lettering to the business exchange, which is the bug).
        await Task.Delay(3000);

        // Assert — A saw all MaxRetries attempts (retries work for A).
        FailingHandlerA.AttemptCount.Should().Be(maxRetries,
            "handler A should be invoked exactly MaxRetries times before the message is dead-lettered");

        // Assert — B received the message exactly ONCE: the original delivery, not A's retries.
        // If this is 3 (or any value > 1), retries are broadcasting to other consumers on the
        // same exchange (the bug the retry-isolation fix prevents).
        SucceedingHandlerB.ReceivedCount.Should().Be(1,
            "handler B should receive the message ONCE (the original delivery), not the retries. " +
            "If it received more, retries are broadcasting to other consumers on the same exchange (bug).");

        SucceedingHandlerB.ReceivedIds.Should().ContainSingle(id => id == eventId,
            "handler B should have received exactly the published event id, exactly once.");

        await host.StopAsync();
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