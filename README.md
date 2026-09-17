Production-ready RabbitMQ client library for .NET 8/9 with multi-connection support, batch consumers, event versioning, and OpenTelemetry instrumentation.

Features
Multi-Connection: Connect to multiple RabbitMQ brokers/vhosts simultaneously
Typed Handlers: DI-based dispatch via IRabbitHandler<T> and IBatchRabbitHandler<T>
Batch Consumer: Buffer messages and process in batches with configurable size/timeout
Event Versioning: Auto-upgrade old event versions to the latest via IEventUpgrader<TFrom, TTo> chains
Retry + DLQ: Configurable retry with exponential delays and dead-letter queues
Publisher Confirms: Reliable publishing with per-message ACK/NACK and timeout
Channel Pooling: Reusable AMQP channels per producer — eliminates the #1 throughput bottleneck (channel-per-publish)
OpenTelemetry: Built-in ActivitySource (tracing) and Meter (metrics)
Health Checks: Per-connection and aggregate health check endpoints
Configurable OTel Names: Use your own instrumentation name via WithInstrumentationName()
Installation
bash

dotnet add package Apymsa.RabbitMQ
Quick Start
appsettings.json
json

{
  "RabbitMQ": {
    "Connections": {
      "main": {
        "HostName": "localhost",
        "Port": 5672,
        "UserName": "guest",
        "Password": "guest",
        "VirtualHost": "/"
      }
    },
    "Producers": [
      {
        "ServiceKey": "orders",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "ExchangeType": "direct",
        "RoutingKey": "order-created"
      }
    ],
    "Consumers": [
      {
        "ServiceKey": "orders-consumer",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "QueueName": "orders.created",
        "RoutingKey": "order-created",
        "PrefetchCount": 10,
        "MaxRetries": 3
      }
    ]
  }
}
Program.cs
csharp

using Apymsa.RabbitMQ.Abstractions;

// Register RabbitMQ
builder.Services.AddRabbitMQ(builder.Configuration);

// Register handlers
builder.Services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer");

// Optional: Configure OpenTelemetry
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(RabbitMqActivitySource.SourceName))
    .WithMetrics(m => m.AddMeter(RabbitMqMetrics.MeterName));
Publish an Event
csharp

public class OrderCreatedEvent : IIntegrationEvent
{
    public Guid OrderId { get; init; }
    public string CustomerEmail { get; init; } = "";
}

public class MyService(IEventPublisher publisher)
{
    public async Task CreateOrderAsync()
    {
        await publisher.PublishAsync(new OrderCreatedEvent
        {
            OrderId = Guid.NewGuid(),
            CustomerEmail = "customer@example.com"
        });
    }
}
Handle an Event
csharp

public class OrderCreatedHandler : IRabbitHandler<OrderCreatedEvent>
{
    public string ConsumerKey => "orders-consumer";
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) => _logger = logger;

    public Task HandleAsync(OrderCreatedEvent @event, MessageContext context)
    {
        _logger.LogInformation(
            "Processing order {OrderId} (correlation={CorrelationId})",
            @event.OrderId, context.CorrelationId);
        return Task.CompletedTask;
    }
}
Batch Consumer
json

{
  "Consumers": [{
    "ServiceKey": "notifications-batch",
    "ConnectionName": "main",
    "ExchangeName": "notifications",
    "QueueName": "notifications.pending",
    "RoutingKey": "#",
    "EnableBatchConsumer": true,
    "BatchSize": 50,
    "BatchTimeoutMs": 3000,
    "PrefetchCount": 100
  }]
}
csharp

builder.Services.AddBatchRabbitHandler<NotificationBatchHandler>("notifications-batch");

public class NotificationBatchHandler : IBatchRabbitHandler<NotificationEvent>
{
    public string ConsumerKey => "notifications-batch";

    public Task HandleBatchAsync(
        IReadOnlyList<NotificationEvent> events,
        IReadOnlyList<MessageContext> contexts)
    {
        // Bulk insert — much faster than individual processing
        return Task.CompletedTask;
    }
}
Channel Pooling
By default, each producer maintains a pool of 4 AMQP channels that are reused across publishes.
This eliminates the AMQP round-trip overhead of creating and closing a channel per message — the
#1 throughput bottleneck in channel-per-publish strategies.

How it works
Rent/Return: Each publish rents a channel from the pool, publishes, then returns it.
Backpressure: A SemaphoreSlim gates concurrency. When all channels are rented, callers await.
Health-aware: Closed channels (e.g. after connection loss) are discarded. Fresh ones are created on next rental.
Faulted channels: After a publisher confirm timeout, the channel is in a dirty state and is discarded — never reused.
Configuration
json

{
  "Producers": [{
    "ServiceKey": "high-throughput",
    "ConnectionName": "main",
    "ExchangeName": "events",
    "RoutingKey": "#",
    "ChannelPoolSize": 8
  }]
}
Pool Size
Confirms
Estimated Throughput
4	Yes	40k–80k msg/s
8	Yes	80k–150k msg/s
4	No	100k–200k msg/s
0	—	Channel-per-publish (backward compat)

Set ChannelPoolSize: 0 to disable pooling and revert to the channel-per-publish strategy.

Event Versioning
csharp

[EventVersion("OrderCreated", 1)]
public record OrderCreatedV1(Guid OrderId, string Email);

[EventVersion("OrderCreated", 2)]
public record OrderCreatedV2(Guid OrderId, string Email, string Phone);

public class OrderV1ToV2Upgrader : IEventUpgrader<OrderCreatedV1, OrderCreatedV2>
{
    public string EventName => "OrderCreated";
    public int FromVersion => 1;
    public int ToVersion => 2;

    public OrderCreatedV2 Upgrade(OrderCreatedV1 source)
        => new(source.OrderId, source.Email, Phone: "N/A");
}

// Register
builder.Services.AddEventUpgrader<OrderV1ToV2Upgrader>();
Configuration
Setting
Default
Description
Connections	—	Named broker connections
Producers	—	Exchange/routing per producer key
Consumers	—	Queue/binding/retry per consumer key
InstrumentationName	"RabbitMQ"	OTel ActivitySource & Meter name

Producer Options
Option
Default
Description
ServiceKey	—	Unique producer identifier
ConnectionName	—	Reference to a connection
ExchangeName	—	Target exchange
ExchangeType	"direct"	Exchange type
RoutingKey	—	Default routing key
Mandatory	true	Fail if unroutable
EnablePublisherConfirms	true	Wait for broker ACK
PublishConfirmTimeoutMs	5000	Confirm timeout
AutoDeclareTopology	true	Auto-declare exchange
ChannelPoolSize	4	Channel pool size (0 = channel-per-publish)

Consumer Options
Option
Default
Description
ServiceKey	—	Unique consumer identifier
ConnectionName	—	Reference to a connection
QueueName	—	Queue to consume from
PrefetchCount	10	AMQP prefetch window
MaxConcurrentHandlers	0	Handler-level concurrency limit
EnableRetry	true	Enable retry on failure
MaxRetries	3	Max delivery attempts
RetryDelays	[0s, 5s, 30s]	Delay per retry
EnableDeadLetter	true	Configure DLX/DLQ
EnableBatchConsumer	false	Use batch mode
BatchSize	10	Messages per batch
BatchTimeoutMs	5000	Batch flush timeout

Metrics
All metrics are exposed via System.Diagnostics.Metrics and collected by any OTel SDK:

Metric
Type
Description
rabbitmq.published	Counter	Messages published
rabbitmq.publish_errors	Counter	Publish failures
rabbitmq.consumed	Counter	Messages consumed
rabbitmq.consume_errors	Counter	Handler failures
rabbitmq.retried	Counter	Retried messages
rabbitmq.dead_lettered	Counter	Dead-lettered messages
rabbitmq.processing_duration_ms	Histogram	Handler execution time
rabbitmq.publish_duration_ms	Histogram	Publish round-trip time
rabbitmq.batches_dispatched	Counter	Batches dispatched (batch consumer)
rabbitmq.batch_size	Histogram	Messages per batch
rabbitmq.channel_pool.rented	Counter	Channel rentals from pool
rabbitmq.channel_pool.returned	Counter	Channels returned to pool
rabbitmq.channel_pool.discarded	Counter	Channels discarded (faulted)
rabbitmq.channel_pool.created	Counter	New channels created by pool

License
MIT