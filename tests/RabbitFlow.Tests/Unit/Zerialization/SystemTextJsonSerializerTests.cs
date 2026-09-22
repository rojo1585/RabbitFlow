using FluentAssertions;
using RabbitFlow.Abstractions;
using RabbitFlow.Infrastructure.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Zerialization;


public class SystemTextJsonSerializerTests
{
    private readonly SystemTextJsonSerializer _serializer = new();
    private record TestEvent(string Name, int Value) : IIntegrationEvent;
    private record NestedEvent(Guid Id, TestEvent Inner) : IIntegrationEvent;
    private record EmptyEvent : IIntegrationEvent;

    [Fact]
    public void Serialize_Deserialize_RoundTrip_PreservesData()
    {
        var original = new TestEvent("orders", 42);
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize<TestEvent>(bytes);

        result.Should().NotBeNull();
        result!.Name.Should().Be("orders");
        result.Value.Should().Be(42);
    }

    [Fact]
    public void Serialize_Deserialize_NestedEvent_PreservesData()
    {
        var original = new NestedEvent(Guid.NewGuid(), new TestEvent("inner", 99));
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize<NestedEvent>(bytes);

        result.Should().NotBeNull();
        result!.Inner.Name.Should().Be("inner");
        result.Inner.Value.Should().Be(99);
    }

    [Fact]
    public void Serialize_Deserialize_EmptyEvent_Works()
    {
        var original = new EmptyEvent();
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize<EmptyEvent>(bytes);

        result.Should().NotBeNull();
    }

    [Fact]
    public void Serialize_ProducesEnvelope_WithEventType()
    {
        var bytes = _serializer.Serialize(new TestEvent("x", 1));
        var envelope = _serializer.DeserializeEnvelope(bytes);

        envelope.Should().NotBeNull();
        envelope!.EventType.Should().Be(typeof(TestEvent).FullName);
    }

    [Fact]
    public void Serialize_ProducesEnvelope_WithEventVersion1()
    {
        var bytes = _serializer.Serialize(new TestEvent("x", 1));
        var envelope = _serializer.DeserializeEnvelope(bytes);

        envelope!.EventVersion.Should().Be(1);
    }

    [Fact]
    public void Deserialize_WithRuntimeType_PreservesData()
    {
        var original = new TestEvent("runtime", 77);
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize(bytes, typeof(TestEvent));

        result.Should().NotBeNull();
        result.Should().BeOfType<TestEvent>();
        var typed = (TestEvent)result!;
        typed.Name.Should().Be("runtime");
        typed.Value.Should().Be(77);
    }

    [Fact]
    public void Deserialize_InvalidJson_ReturnsNull()
    {
        var badBytes = Encoding.UTF8.GetBytes("not json");
        var result = _serializer.Deserialize<TestEvent>(badBytes);
        result.Should().BeNull();
    }

    [Fact]
    public void Deserialize_WithRuntimeType_InvalidJson_ReturnsNull()
    {
        var badBytes = Encoding.UTF8.GetBytes("not json");
        var result = _serializer.Deserialize(badBytes, typeof(TestEvent));
        result.Should().BeNull();
    }

    [Fact]
    public void Serialize_NonNull_ProducesNonEmptyBytes()
    {
        var bytes = _serializer.Serialize(new TestEvent("a", 1));
        bytes.Length.Should().BeGreaterThan(0);
    }
}
