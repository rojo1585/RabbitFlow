using FluentAssertions;
using RabbitFlow.Infrastructure.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Consuming;


public class HandlerTypeRegistryTests
{
    private class OrderCreatedEvent { }
    private class OrderCreatedHandler { }
    private class PaymentProcessedEvent { }
    private class PaymentHandler { }
    private class BatchOrderHandler { }

    [Fact]
    public void Resolve_ReturnsHandler_WhenRegistered()
    {
        var registration = new HandlerRegistration(
            "orders-consumer",
            typeof(OrderCreatedHandler),
            typeof(OrderCreatedEvent),
            typeof(OrderCreatedEvent).FullName!,
            IsBatch: false);

        var registry = new HandlerTypeRegistry([registration]);

        var result = registry.Resolve("orders-consumer", typeof(OrderCreatedEvent).FullName!);
        result.Should().NotBeNull();
        result!.Value.HandlerType.Should().Be(typeof(OrderCreatedHandler));
        result.Value.EventType.Should().Be(typeof(OrderCreatedEvent));
        result.Value.IsBatch.Should().BeFalse();
    }

    [Fact]
    public void Resolve_ReturnsNull_WhenNotRegistered()
    {
        var registry = new HandlerTypeRegistry([]);
        var result = registry.Resolve("unknown", "unknown");
        result.Should().NotBeNull();
        result.HasValue.Should().BeTrue();
        result.Value.HandlerType.Should().BeNull();
        result.Value.EventType.Should().BeNull();
        result.Value.IsBatch.Should().BeFalse();
    }

    [Fact]
    public void Resolve_ReturnsBatchHandler_WhenRegisteredAsBatch()
    {
        var registration = new HandlerRegistration(
            "batch-consumer",
            typeof(BatchOrderHandler),
            typeof(OrderCreatedEvent),
            typeof(OrderCreatedEvent).FullName!,
            IsBatch: true
        );

        var registry = new HandlerTypeRegistry([registration]);

        var result = registry.Resolve("batch-consumer", typeof(OrderCreatedEvent).FullName!);
        result.Should().NotBeNull();
        result!.Value.IsBatch.Should().BeTrue();
    }

    [Fact]
    public void Resolve_SupportsMultipleConsumers_ForSameEvent()
    {
        var registrations = new[]
        {
        new HandlerRegistration("consumer-a", typeof(OrderCreatedHandler), typeof(OrderCreatedEvent), typeof(OrderCreatedEvent).FullName!, false),
        new HandlerRegistration("consumer-b", typeof(PaymentHandler), typeof(OrderCreatedEvent), typeof(OrderCreatedEvent).FullName!, false),
        };

        var registry = new HandlerTypeRegistry(registrations);

        var resultA = registry.Resolve("consumer-a", typeof(OrderCreatedEvent).FullName!);
        var resultB = registry.Resolve("consumer-b", typeof(OrderCreatedEvent).FullName!);

        resultA.Should().NotBeNull();
        resultA!.Value.HandlerType.Should().Be(typeof(OrderCreatedHandler));

        resultB.Should().NotBeNull();
        resultB!.Value.HandlerType.Should().Be(typeof(PaymentHandler));
    }

    [Fact]
    public void Resolve_SupportsDifferentEvents_ForSameConsumer()
    {
        var registrations = new[]
        {
        new HandlerRegistration("multi-consumer", typeof(OrderCreatedHandler), typeof(OrderCreatedEvent), typeof(OrderCreatedEvent).FullName!, false),
        new HandlerRegistration("multi-consumer", typeof(PaymentHandler), typeof(PaymentProcessedEvent), typeof(PaymentProcessedEvent).FullName!, false),
        };

        var registry = new HandlerTypeRegistry(registrations);

        var orderResult = registry.Resolve("multi-consumer", typeof(OrderCreatedEvent).FullName!);
        var paymentResult = registry.Resolve("multi-consumer", typeof(PaymentProcessedEvent).FullName!);

        orderResult!.Value.HandlerType.Should().Be(typeof(OrderCreatedHandler));
        paymentResult!.Value.HandlerType.Should().Be(typeof(PaymentHandler));
    }

    [Fact]
    public void Constructor_Throws_OnDuplicateRegistration()
    {
        var registrations = new[]
        {
        new HandlerRegistration("consumer", typeof(OrderCreatedHandler), typeof(OrderCreatedEvent), typeof(OrderCreatedEvent).FullName!, false),
        new HandlerRegistration("consumer", typeof(PaymentHandler), typeof(OrderCreatedEvent), typeof(OrderCreatedEvent).FullName!, false),
    };

        var act = () => new HandlerTypeRegistry(registrations);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*Duplicate*");
    }
    [Fact]
    public void Freeze_DoesNotThrow()
    {
        var registry = new HandlerTypeRegistry([]);
        var act = () => registry.Freeze();
        act.Should().NotThrow();
    }
}
