using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;

namespace RabbitFlow.Infrastructure.Connection;
/// <summary>
/// Manages multiple named <see cref="ManagedConnection"/> instances.
/// Implements <see cref="IRabbitConnectionRegistry"/> for health checks and status queries.
/// 
/// <para>
/// Registered as a Singleton.
/// The <see cref="ConnectionInitializerHostedService"/> calls <see cref="StartAll"/>
/// at application startup to begin connecting.
/// </para>
/// </summary>
public sealed class RabbitConnectionRegistry : IRabbitConnectionRegistry, IAsyncDisposable
{
    private readonly Dictionary<string, ManagedConnection> _connections = new();
    private readonly ILogger<RabbitConnectionRegistry> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly RabbitMqSettings _settings;
    private bool _started;
    private bool _disposed;

    /// <summary>
    /// Creates a new registry. The connections are NOT started yet —
    /// call <see cref="StartAll"/> to begin connecting.
    /// </summary>
    public RabbitConnectionRegistry(IOptions<RabbitMqSettings> settings, ILoggerFactory loggerFactory)
    {
        _settings = settings.Value;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RabbitConnectionRegistry>();

        CreateManagedConnections();
    }

    /// <summary>
    /// Gets a managed connection by name.
    /// Throws if the connection name is not registered.
    /// </summary>
    /// <param name="connectionName">The connection name.</param>
    /// <returns>The managed connection.</returns>
    /// <exception cref="Exceptions.ConnectionNotFoundException">
    /// Thrown when the connection name is not found.
    /// </exception>
    public ManagedConnection GetConnection(string connectionName)
    {
        if (_connections.TryGetValue(connectionName, out var conn))
            return conn;

        throw new Exceptions.ConnectionNotFoundException(connectionName);
    }

    /// <inheritdoc/>
    public bool IsConnected(string connectionName)
    {
        if (_connections.TryGetValue(connectionName, out var conn))
            return conn.IsConnected;

        _logger.LogWarning("Connection '{Name}' not found in registry", connectionName);
        return false;
    }

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, bool> GetAllConnectionStates()
    {
        return _connections.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.IsConnected);
    }

    /// <summary>
    /// Starts all managed connections. Called by the hosted service at startup.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if called more than once.</exception>
    public void StartAll()
    {
        if (_started)
            throw new InvalidOperationException("StartAll has already been called.");

        _started = true;

        foreach (var (name, conn) in _connections)
        {
            _logger.LogInformation("Starting connection '{Name}'...", name);
            conn.Start();
        }

        _logger.LogInformation("{Count} connection(s) starting in background", _connections.Count);
    }

    /// <summary>
    /// Creates <see cref="ManagedConnection"/> instances from the configuration.
    /// </summary>
    private void CreateManagedConnections()
    {
        if (_settings.Connections.Count == 0)
        {
            _logger.LogWarning("No RabbitMQ connections configured in '{Section}'", RabbitMqSettings.SectionName);
            return;
        }

        foreach (var (name, options) in _settings.Connections)
        {
            var managed = new ManagedConnection(name, options, _loggerFactory.CreateLogger<ManagedConnection>());

            _connections[name] = managed;

            _logger.LogDebug("Registered connection '{Name}' → {Host}:{Port}/{VHost}", name, options.HostName, options.Port, options.VirtualHost);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogInformation("Disposing {Count} connection(s)...", _connections.Count);

        var tasks = _connections.Values.Select(c => c.DisposeAsync().AsTask()).ToArray();
        await Task.WhenAll(tasks).ConfigureAwait(false);

        _connections.Clear();
    }
}