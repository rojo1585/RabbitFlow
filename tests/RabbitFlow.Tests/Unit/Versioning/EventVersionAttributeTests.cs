using FluentAssertions;
using RedRabbit.Diagnostics;

namespace RedRabbit.Tests.Unit.Versioning;


public class EventVersionAttributeTests
{
    [Fact]
    public void Constructor_SetsEventName()
    {
        var attr = new EventVersionAttribute("OrderCreated", 2);
        attr.EventName.Should().Be("OrderCreated");
    }

    [Fact]
    public void Constructor_SetsVersion()
    {
        var attr = new EventVersionAttribute("OrderCreated", 3);
        attr.Version.Should().Be(3);
    }

    [Fact]
    public void Constructor_DefaultVersion_Is1()
    {
        var attr = new EventVersionAttribute("OrderCreated");
        attr.Version.Should().Be(1);
    }

    [Fact]
    public void Constructor_NullEventName_Throws()
    {
        var act = () => new EventVersionAttribute(null!);
        act.Should().Throw<ArgumentNullException>();
    }
}
