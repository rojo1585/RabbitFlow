namespace RedRabbit.Infrastructure.Consuming;

/// <summary>
/// Fits the consumers graceful-shutdown work inside the host's shutdown timeout.
/// <para>
/// The generic host gives all hosted services <c>HostOptions.ShutdownTimeout</c> (30s by
/// default) to stop. If a consumer drains for longer, the host stops waiting and the process
/// may exit while ACK/NACKs are still being sent — exactly what the drain is meant to avoid.
/// The drain must therefore end early enough to leave room for the work that follows it:
/// cancelling the consumer, NACKing abandoned messages, closing channels and connections.
/// </para>
/// </summary>
internal static class ShutdownBudget
{
    /// <summary>
    /// The .NET default for <c>HostOptions.ShutdownTimeout</c>, used when the host's value
    /// cannot be determined.
    /// </summary>
    public static readonly TimeSpan DefaultHostShutdownTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Time reserved, out of the host's shutdown timeout, for the steps that run after the
    /// drain: consumer cancellation, requeue NACKs for abandoned messages, retry-channel and
    /// channel close (each individually bounded to a few seconds).
    /// </summary>
    public static readonly TimeSpan PostDrainReserve = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Returns the drain timeout a consumer can actually use: the configured value, capped so
    /// that drain + <see cref="PostDrainReserve"/> fits in <paramref name="hostShutdownTimeout"/>.
    /// Never negative; zero means "abandon in-flight handlers immediately and requeue".
    /// </summary>
    public static TimeSpan GetEffectiveDrainTimeout(TimeSpan configuredDrainTimeout, TimeSpan hostShutdownTimeout)
    {
        if (configuredDrainTimeout <= TimeSpan.Zero)
            return TimeSpan.Zero;

        if (hostShutdownTimeout == Timeout.InfiniteTimeSpan || hostShutdownTimeout >= TimeSpan.MaxValue - PostDrainReserve)
            return configuredDrainTimeout;

        var available = hostShutdownTimeout - PostDrainReserve;
        if (available <= TimeSpan.Zero)
            return TimeSpan.Zero;

        return configuredDrainTimeout <= available ? configuredDrainTimeout : available;
    }

    /// <summary>
    /// Reads <c>HostOptions.ShutdownTimeout</c> from the container when the app runs on the
    /// .NET generic host; otherwise returns <see cref="DefaultHostShutdownTimeout"/>.
    /// </summary>
    /// <remarks>
    /// <c>HostOptions</c> lives in the full <c>Microsoft.Extensions.Hosting</c> package, which
    /// this library deliberately does not depend on (only on its abstractions). Every app that
    /// actually hosts the consumers (<c>Host.CreateDefaultBuilder</c>, <c>WebApplication</c>,
    /// workers) has it loaded, so it is resolved by name at runtime.
    /// </remarks>
    public static TimeSpan GetHostShutdownTimeout(IServiceProvider serviceProvider)
    {
        try
        {
            var hostOptionsType = Type.GetType("Microsoft.Extensions.Hosting.HostOptions, Microsoft.Extensions.Hosting", throwOnError: false);
            if (hostOptionsType is null)
                return DefaultHostShutdownTimeout;

            var optionsType = typeof(Microsoft.Extensions.Options.IOptions<>).MakeGenericType(hostOptionsType);
            var options = serviceProvider.GetService(optionsType);
            var hostOptions = optionsType.GetProperty("Value")?.GetValue(options);
            var timeout = hostOptionsType.GetProperty("ShutdownTimeout")?.GetValue(hostOptions);

            return timeout is TimeSpan value ? value : DefaultHostShutdownTimeout;
        }
        catch (Exception)
        {
            return DefaultHostShutdownTimeout;
        }
    }
}
