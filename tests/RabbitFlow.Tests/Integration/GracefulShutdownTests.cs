using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
using System.Reflection;

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

    /// <summary>
    /// Handler that simulates a non-cancellable unit of work (like a DB transaction
    /// that must commit once started). The <see cref="Task.Delay"/> does NOT observe
    /// the cancellation token, so the handler runs to completion regardless of
    /// shutdown signalling. Used by <see cref="ShutdownDrain_AllowsInProgressHandlers_ToComplete"/>.
    /// </summary>
    private class UncancellableSlowHandler : IRabbitHandler<ShutdownEvent>
    {
        public string ConsumerKey => "shutdown-consumer";
        public static int ProcessedCount;
        public static readonly List<Guid> ProcessedIds = [];
        public static int StartedCount;

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            // Do NOT pass cancellationToken — simulate a handler that doesn't check cancellation
            // (like a DB transaction that must complete once started).
            await Task.Delay(TimeSpan.FromSeconds(2));
            Interlocked.Increment(ref ProcessedCount);
            ProcessedIds.Add(@event.Id);
        }
    }

    /// <summary>
    /// Handler that takes longer (10s) than the configured <c>ShutdownDrainTimeout</c>.
    /// Does NOT observe the cancellation token, so it will only complete if the consumer
    /// waits for it. Used by <see cref="ShutdownDrain_NacksUnfinishedMessages_WithRequeue_AfterTimeout"/>
    /// to verify that after the drain timeout, the unfinished handler is abandoned
    /// (NACK-with-requeue) and shutdown proceeds without hanging.
    /// </summary>
    private class BlockingHandler : IRabbitHandler<ShutdownEvent>
    {
        public string ConsumerKey => "shutdown-consumer";
        public static int StartedCount;
        public static int CompletedCount;

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            // Takes 10s, does NOT check cancellationToken — longer than ShutdownDrainTimeout.
            await Task.Delay(TimeSpan.FromSeconds(10));
            Interlocked.Increment(ref CompletedCount);
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

    /// <summary>
    /// Verifies the happy-path of graceful-shutdown drain: when <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/>
    /// is generous (10s) and the in-flight handlers complete within that window (2s each),
    /// all handlers should run to completion (NOT be cancelled) and shutdown should complete
    /// well under the drain timeout + margin.
    /// </summary>
    [Fact]
    public async Task ShutdownDrain_AllowsInProgressHandlers_ToComplete()
    {
        // Arrange — reset static state
        UncancellableSlowHandler.ProcessedCount = 0;
        UncancellableSlowHandler.ProcessedIds.Clear();
        UncancellableSlowHandler.StartedCount = 0;

        const string exchangeName = "shutdown-drain-test";
        const string queueName = "shutdown-drain-queue";

        using var host = await BuildHostAsync(
            shutdownDrainTimeout: TimeSpan.FromSeconds(10),
            exchangeName: exchangeName,
            queueName: queueName,
            handlerType: typeof(UncancellableSlowHandler));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        // Act: publish 3 messages.
        foreach (var id in eventIds)
            await publisher.PublishAsync(new ShutdownEvent(id));

        // Wait for all 3 handlers to be in-flight (each takes 2s, prefetch=10 so all 3 start concurrently).
        await WaitForAsync(() => UncancellableSlowHandler.StartedCount >= 3, TimeSpan.FromSeconds(5));

        // Stop the host — should drain the in-flight handlers (up to 10s).
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync();
        sw.Stop();

        // Assert: all 3 handlers completed (NOT cancelled mid-flight).
        UncancellableSlowHandler.ProcessedCount.Should().Be(3,
            "all in-flight handlers should complete during the drain timeout, not be cancelled");
        UncancellableSlowHandler.ProcessedIds.Should().BeEquivalentTo(eventIds,
            "all 3 published event ids should have been processed by the handler");

        // Assert: shutdown completed within the drain timeout + margin (handler 2s + cleanup, well under 15s).
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
            "shutdown should complete within ShutdownDrainTimeout + margin, not hang");
    }

    /// <summary>
    /// Verifies the timeout-path of graceful-shutdown drain: when <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/>
    /// is short (1s) and the in-flight handler takes longer (10s), shutdown must NOT hang
    /// waiting for the handler. After the drain timeout, the unfinished message is NACKed
    /// with requeue=true (so the broker redelivers it on the next consumer start) and the
    /// channel is closed.
    /// </summary>
    [Fact]
    public async Task ShutdownDrain_NacksUnfinishedMessages_WithRequeue_AfterTimeout()
    {
        // Arrange — reset static state
        BlockingHandler.StartedCount = 0;
        BlockingHandler.CompletedCount = 0;

        const string exchangeName = "shutdown-drain-timeout-test";
        const string queueName = "shutdown-drain-timeout-queue";

        using var host = await BuildHostAsync(
            shutdownDrainTimeout: TimeSpan.FromSeconds(1),
            exchangeName: exchangeName,
            queueName: queueName,
            handlerType: typeof(BlockingHandler));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act: publish 1 message.
        await publisher.PublishAsync(new ShutdownEvent(Guid.NewGuid()));

        // Wait for the handler to start (it takes 10s, longer than the 1s drain timeout).
        await WaitForAsync(() => BlockingHandler.StartedCount >= 1, TimeSpan.FromSeconds(5));

        // Stop the host — should drain for 1s, then NACK with requeue and close.
        // Use Task.WhenAny with a hard 15s ceiling so the test does not hang indefinitely
        // if the drain logic regresses; the actual assertion is < 5s.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var stopTask = host.StopAsync();
        var winner = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(15)));
        sw.Stop();

        // Assert: shutdown completed (didn't hang waiting for the 10s handler).
        // Task.WhenAny returns the task that completed first by reference — if stopTask
        // didn't finish before the 15s ceiling, winner would be the delay task.
        (winner == stopTask).Should().BeTrue(
            "shutdown should complete within the drain timeout + margin, not hang waiting for the 10s handler");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5),
            "shutdown should complete within ~5s (drain timeout 1s + cleanup), not wait for the full 10s handler");

        // Assert: handler was started exactly once (not retried multiple times during shutdown).
        BlockingHandler.StartedCount.Should().Be(1,
            "the handler should have been invoked exactly once — no spurious reprocessing during shutdown");

        // Assert: handler was NOT completed (it was abandoned after the drain timeout and NACKed with requeue).
        BlockingHandler.CompletedCount.Should().Be(0,
            "the blocking handler should NOT have completed — it should be abandoned after the drain timeout " +
            "and the message NACKed with requeue so the broker can redeliver it on the next consumer start");
    }

    /// <summary>
    /// Builds a host with parameterized <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/>
    /// and unique exchange/queue names to avoid cross-test interference. The handler type is
    /// registered via reflection because <see cref="RabbitHandlerExtensions.AddRabbitHandler{THandler}"/>
    /// is generic and the handler type is only known at runtime (passed in as <see cref="Type"/>).
    /// </summary>
    private async Task<IHost> BuildHostAsync(TimeSpan shutdownDrainTimeout, string exchangeName, string queueName, Type handlerType)
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
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    QueueName = queueName,
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    // Property added by the parallel p4-shutdown-consumer work: maximum time the
                    // consumer waits for in-flight handlers to complete before NACKing them
                    // with requeue=true and closing the channel during graceful shutdown.
                    ShutdownDrainTimeout = shutdownDrainTimeout
                });
            });

            // Reflectively invoke the generic AddRabbitHandler<THandler>(consumerKey) extension.
            // The method is generic (THandler) so we cannot call it directly with a runtime Type;
            // MakeGenericMethod closes the open generic with the concrete handler type.
            var addRabbitHandlerMethod = typeof(RabbitHandlerExtensions)
                .GetMethod(nameof(RabbitHandlerExtensions.AddRabbitHandler), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(handlerType);

            addRabbitHandlerMethod.Invoke(null, [services, "shutdown-consumer"]);
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        // Give the consumer time to declare topology & start consuming.
        await Task.Delay(2000);
        return host;
    }

    /// <summary>
    /// Polls <paramref name="condition"/> at a fixed interval until it returns <c>true</c>
    /// or <paramref name="timeout"/> expires. Throws <see cref="TimeoutException"/> on timeout.
    /// Local copy of the boolean overload (the generic <c>WaitForAsync&lt;T&gt;</c> overload
    /// would return immediately on <c>false</c> because <c>false is not null</c>).
    /// </summary>
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

