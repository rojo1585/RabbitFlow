using DotNet.Testcontainers.Builders;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testcontainers.RabbitMq;

namespace RabbitFlow.Tests.Integration;



/// <summary>
/// Shared RabbitMQ container fixture for integration tests.
/// The container is started once and shared across all tests in the collection.
/// </summary>
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private const string TestUserName = "testuser";
    private const string TestPassword = "testpass";

    private readonly RabbitMqContainer _container = new RabbitMqBuilder()
     .WithImage("rabbitmq:3.13-management-alpine")
     .WithUsername(TestUserName)
     .WithPassword(TestPassword)
     .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"))
     .Build();

    public string HostName => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);
    public string UserName => TestUserName;
    public string Password => TestPassword;

    /// <summary>
    /// Creates a raw RabbitMQ connection for setup/teardown in tests
    /// (e.g., declaring queues, purging, etc.).
    /// </summary>
    public async Task<IConnection> CreateRawConnectionAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = HostName,
            Port = Port,
            UserName = UserName,
            Password = Password
        };
        return await factory.CreateConnectionAsync();
    }

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name, DisableParallelization = true)]
public class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>
{
    public const string Name = "RabbitMQ Integration";
}
