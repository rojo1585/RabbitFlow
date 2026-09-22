using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Diagnostics;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Versioning;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Versioning;

public class EventUpgraderRegistryTests
{


    [EventVersion("OrderCreated", 1)]
    private record OrderV1(Guid Id);

    [EventVersion("OrderCreated", 2)]
    private record OrderV2(Guid Id, string Email);

    [EventVersion("OrderCreated", 3)]
    private record OrderV3(Guid Id, string Email, string Phone);

    private static UpgraderEntry MakeEntry(string eventName,
                                           int fromVer,
                                           int toVer,
                                           Type fromType,
                                           Type toType,
                                           Type upgraderType,
                                           Func<object, object, object> upgradeFunc)
        => new(eventName, fromVer, toVer, fromType, toType, upgraderType, upgradeFunc);

    [Fact]
    public void GetHighestVersion_ReturnsHighestVersion()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);

        registry.GetHighestVersion("OrderCreated").Should().Be(2);
    }

    [Fact]
    public void GetHighestVersion_Returns1_WhenNoChain()
    {
        var registry = new EventUpgraderRegistry([]);
        registry.GetHighestVersion("Unknown").Should().Be(1);
    }

    [Fact]
    public void GetHighestVersion_Returns3_ForThreeStepChain()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);

        registry.GetHighestVersion("OrderCreated").Should().Be(3);
    }

    [Fact]
    public void GetTypeForVersion_ReturnsCorrectType()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);

        registry.GetTypeForVersion("OrderCreated", 1).Should().Be(typeof(OrderV1));
        registry.GetTypeForVersion("OrderCreated", 2).Should().Be(typeof(OrderV2));
        registry.GetTypeForVersion("OrderCreated", 3).Should().Be(typeof(OrderV3));
    }

    [Fact]
    public void GetTypeForVersion_ReturnsNull_ForUnknownVersion()
    {
        var registry = new EventUpgraderRegistry([]);
        registry.GetTypeForVersion("Unknown", 1).Should().BeNull();
    }

    [Fact]
    public void GetLatestType_ReturnsHighestVersionType()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);

        registry.GetLatestType("OrderCreated").Should().Be(typeof(OrderV3));
    }

    [Fact]
    public void Upgrade_AppliesSingleStep()
    {
        var v1 = new OrderV1(Guid.NewGuid());
        var upgraded = new OrderV2(v1.Id, "default@email.com");

        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object),
                (_, e) => new OrderV2(((OrderV1)e).Id, "default@email.com"))
        };
        var registry = new EventUpgraderRegistry(entries);

        var services = new ServiceCollection();

        services.AddSingleton<object>(new object());

        var serviceProvider = services.BuildServiceProvider(); ;
        var result = registry.Upgrade("OrderCreated", v1, 1, serviceProvider);

        result.Should().BeOfType<OrderV2>();
        var v2 = (OrderV2)result;
        v2.Id.Should().Be(v1.Id);
        v2.Email.Should().Be("default@email.com");
    }

    [Fact]
    public void Upgrade_AppliesFullChain()
    {
        var v1 = new OrderV1(Guid.NewGuid());

        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object),
                (_, e) => new OrderV2(((OrderV1)e).Id, "default@email.com")),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object),
                (_, e) => { var v2 = (OrderV2)e; return new OrderV3(v2.Id, v2.Email, "N/A"); })
        };
        var registry = new EventUpgraderRegistry(entries);
        var services = new ServiceCollection();

        services.AddSingleton<object>(new object());

        var serviceProvider = services.BuildServiceProvider();

        var result = registry.Upgrade("OrderCreated", v1, 1, serviceProvider);

        result.Should().BeOfType<OrderV3>();
        var v3 = (OrderV3)result;
        v3.Id.Should().Be(v1.Id);
        v3.Email.Should().Be("default@email.com");
        v3.Phone.Should().Be("N/A");
    }

    [Fact]
    public void Upgrade_ReturnsOriginal_WhenAlreadyAtLatest()
    {
        var v3 = new OrderV3(Guid.NewGuid(), "a@b.com", "123");

        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);

        var services = new ServiceCollection().BuildServiceProvider();
        var result = registry.Upgrade("OrderCreated", v3, 3, services);

        result.Should().BeSameAs(v3);
    }

    [Fact]
    public void Upgrade_ReturnsOriginal_WhenNoChainForEvent()
    {
        var registry = new EventUpgraderRegistry([]);
        var services = new ServiceCollection().BuildServiceProvider();
        var obj = new object();

        var result = registry.Upgrade("Unknown", obj, 1, services);
        result.Should().BeSameAs(obj);
    }

    [Fact]
    public void Constructor_Throws_WhenChainHasGap()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 3, 4, typeof(OrderV3), typeof(OrderV4), typeof(object), (_, e) => e)
        };

        var act = () => new EventUpgraderRegistry(entries);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*not continuous*");
    }

    [Fact]
    public void Constructor_Throws_WhenUpgraderIsSelfLoop()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 1, typeof(OrderV1), typeof(OrderV1), typeof(object), (_, e) => e)
        };

        var act = () => new EventUpgraderRegistry(entries);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*ToVersion must be strictly greater than FromVersion*");
    }

    [Fact]
    public void Constructor_Throws_WhenUpgraderIsDowngrade()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 3, 2, typeof(OrderV3), typeof(OrderV2), typeof(object), (_, e) => e)
        };

        var act = () => new EventUpgraderRegistry(entries);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*ToVersion must be strictly greater than FromVersion*");
    }

    [Fact]
    public void Constructor_Throws_WhenUpgradersFormCycle_V1toV2toV1()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 1, typeof(OrderV2), typeof(OrderV1), typeof(object), (_, e) => e)
        };

        var act = () => new EventUpgraderRegistry(entries);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*ToVersion must be strictly greater than FromVersion*");
    }

    [Fact]
    public void Constructor_Throws_WhenUpgradersFormCycle_V1toV2toV3toV1()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 3, 1, typeof(OrderV3), typeof(OrderV1), typeof(object), (_, e) => e)
        };

        var act = () => new EventUpgraderRegistry(entries);
        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*ToVersion must be strictly greater than FromVersion*");
    }

    [Fact]
    public void Upgrade_Throws_WhenMessageVersionIsNewerThanHighest()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e),
            MakeEntry("OrderCreated", 2, 3, typeof(OrderV2), typeof(OrderV3), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);
        var services = new ServiceCollection().BuildServiceProvider();

        var act = () => registry.Upgrade("OrderCreated", new OrderV3(Guid.NewGuid(), "a@b.com", "123"), 5, services);

        act.Should().Throw<EventVersionNewerThanRegisteredException>()
           .WithMessage("*version 5*highest registered version is 3*");
    }

    [Fact]
    public void Upgrade_Throws_EventVersionNewerThanRegistered_PreservesMetadata()
    {
        var entries = new[]
        {
            MakeEntry("OrderCreated", 1, 2, typeof(OrderV1), typeof(OrderV2), typeof(object), (_, e) => e)
        };
        var registry = new EventUpgraderRegistry(entries);
        var services = new ServiceCollection().BuildServiceProvider();

        try
        {
            registry.Upgrade("OrderCreated", new OrderV2(Guid.NewGuid(), "a@b.com"), 4, services);
        }
        catch (EventVersionNewerThanRegisteredException ex)
        {
            ex.EventName.Should().Be("OrderCreated");
            ex.ReceivedVersion.Should().Be(4);
            ex.HighestRegisteredVersion.Should().Be(2);
        }
    }

    [EventVersion("OrderCreated", 4)]
    private record OrderV4(Guid Id, string Email, string Phone, string Address);
}