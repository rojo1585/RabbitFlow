using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RedRabbit.Infrastructure.Consuming;

namespace RedRabbit.Tests.Unit.Consuming;

public class ShutdownBudgetTests
{
    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    [Fact]
    public void DrainThatFits_IsKept()
    {
        ShutdownBudget.GetEffectiveDrainTimeout(S(20), S(30)).Should().Be(S(20));
    }

    [Fact]
    public void DrainExceedingHostTimeoutMinusReserve_IsCapped()
    {
        // 30s host timeout - 10s reserve = 20s available.
        ShutdownBudget.GetEffectiveDrainTimeout(S(30), S(30)).Should().Be(S(20));
        ShutdownBudget.GetEffectiveDrainTimeout(S(60), S(45)).Should().Be(S(35));
    }

    [Fact]
    public void HostTimeoutNotLongerThanReserve_LeavesNoDrain()
    {
        ShutdownBudget.GetEffectiveDrainTimeout(S(20), S(10)).Should().Be(TimeSpan.Zero);
        ShutdownBudget.GetEffectiveDrainTimeout(S(20), S(5)).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ZeroOrNegativeConfiguredDrain_IsZero()
    {
        ShutdownBudget.GetEffectiveDrainTimeout(TimeSpan.Zero, S(30)).Should().Be(TimeSpan.Zero);
        ShutdownBudget.GetEffectiveDrainTimeout(S(-1), S(30)).Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void InfiniteHostTimeout_ImposesNoLimit()
    {
        ShutdownBudget.GetEffectiveDrainTimeout(S(300), Timeout.InfiniteTimeSpan).Should().Be(S(300));
        ShutdownBudget.GetEffectiveDrainTimeout(S(300), TimeSpan.MaxValue).Should().Be(S(300));
    }

    [Fact]
    public void GetHostShutdownTimeout_ReadsConfiguredHostOptions()
    {
        var services = new ServiceCollection();
        services.Configure<HostOptions>(o => o.ShutdownTimeout = S(12));

        ShutdownBudget.GetHostShutdownTimeout(services.BuildServiceProvider()).Should().Be(S(12));
    }

    [Fact]
    public void GetHostShutdownTimeout_WithoutHostOptions_ReturnsDotNetDefault()
    {
        var services = new ServiceCollection();

        ShutdownBudget.GetHostShutdownTimeout(services.BuildServiceProvider())
            .Should().Be(ShutdownBudget.DefaultHostShutdownTimeout);
    }

    [Fact]
    public void GetHostShutdownTimeout_FromRealHost_MatchesHostOptions()
    {
        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(s => s.Configure<HostOptions>(o => o.ShutdownTimeout = S(42)))
            .Build();

        ShutdownBudget.GetHostShutdownTimeout(host.Services).Should().Be(S(42));
    }
}
