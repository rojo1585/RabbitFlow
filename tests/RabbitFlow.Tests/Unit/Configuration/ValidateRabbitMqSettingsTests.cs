using FluentAssertions;
using Microsoft.Extensions.Options;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;

public class ValidateRabbitMqSettingsTests
{
    private readonly ValidateRabbitMqSettings _validator = new();
    private static RabbitMqSettings ValidSettings() => new()
    {
        Connections = new Dictionary<string, RabbitConnectionOptions>
        {
            ["main"] = new()
            {
                Name = "main",
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            }
        },
        Producers =
        [
            new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created"
        }
        ],
        Consumers =
        [
            new RabbitConsumerOptions
        {
            ServiceKey = "orders-consumer",
            ConnectionName = "main",
            ExchangeName = "orders",
            QueueName = "orders-queue",
            RoutingKey = "order-created"
        }
        ]
    };

    [Fact]
    public void Validate_WithValidSettings_ReturnsSuccess()
    {
        var result = _validator.Validate(Options.DefaultName, ValidSettings());
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithEmptyConfig_ReturnsSuccess()
    {
        var result = _validator.Validate(Options.DefaultName, new RabbitMqSettings());
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithNamedOptions_SkipsValidation()
    {
        var result = _validator.Validate("custom-name", ValidSettings());
        result.Skipped.Should().BeTrue();
    }

    [Fact]
    public void Validate_NoConnectionsButProducersExist_Fails()
    {
        var settings = new RabbitMqSettings
        {
            Producers =
            [
                new RabbitProducerOptions
            {
                ServiceKey = "orders",
                ConnectionName = "missing",
                ExchangeName = "orders",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("no connections");
    }

    [Fact]
    public void Validate_NoConnectionsButConsumersExist_Fails()
    {
        var settings = new RabbitMqSettings
        {
            Consumers =
            [
                new RabbitConsumerOptions
            {
                ServiceKey = "c",
                ConnectionName = "missing",
                ExchangeName = "ex",
                QueueName = "q",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("no connections");
    }

    [Fact]
    public void Validate_DuplicateConnectionNames_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main-dup"] = settings.Connections["main"];

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_DuplicateProducerServiceKeys_Fails()
    {
        var settings = ValidSettings();
        settings.Producers.Add(new RabbitProducerOptions
        {
            ServiceKey = "orders", // dp
            ConnectionName = "main",
            ExchangeName = "orders-2",
            RoutingKey = "order-updated"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate producer ServiceKey");
        result.FailureMessage.Should().Contain("orders");
    }

    [Fact]
    public void Validate_ProducerMissingConnection_Fails()
    {
        var settings = ValidSettings();
        settings.Producers.Add(new RabbitProducerOptions
        {
            ServiceKey = "payments",
            ConnectionName = "nonexistent",
            ExchangeName = "payments",
            RoutingKey = "pay"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("nonexistent");
    }

    [Fact]
    public void Validate_DuplicateConsumerServiceKeys_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers.Add(new RabbitConsumerOptions
        {
            ServiceKey = "orders-consumer", //dp
            ConnectionName = "main",
            ExchangeName = "orders",
            QueueName = "orders-queue-2",
            RoutingKey = "order-created"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate consumer ServiceKey");
        result.FailureMessage.Should().Contain("orders-consumer");
    }

    [Fact]
    public void Validate_ConsumerMissingConnection_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers.Add(new RabbitConsumerOptions
        {
            ServiceKey = "audit-consumer",
            ConnectionName = "nonexistent",
            ExchangeName = "audit",
            QueueName = "audit-queue",
            RoutingKey = "audit"
        });

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("nonexistent");
    }


    [Fact]
    public void Validate_MultipleErrors_ReportsAll()
    {
        var settings = new RabbitMqSettings
        {
            Connections = new Dictionary<string, RabbitConnectionOptions>
            {
                ["main"] = new()
                {
                    Name = "main",
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                }
            },
            Producers =
            [
                new RabbitProducerOptions
            {
                ServiceKey = "dup",
                ConnectionName = "missing1",
                ExchangeName = "ex1",
                RoutingKey = "rk"
            },
            new RabbitProducerOptions
            {
                ServiceKey = "dup", // dp k
                ConnectionName = "missing2",
                ExchangeName = "ex2",
                RoutingKey = "rk"
            }
            ]
        };

        var result = _validator.Validate(Options.DefaultName, settings);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Duplicate producer ServiceKey");
        result.FailureMessage.Should().Contain("missing1");
        result.FailureMessage.Should().Contain("missing2");
    }

    // ─── Bounds validation tests (B3) ──────────────────────────────────

    [Fact]
    public void Validate_ConsumerPrefetchCountZero_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with { PrefetchCount = 0 };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("PrefetchCount must be >= 1");
    }

    [Fact]
    public void Validate_ConsumerBatchSizeZeroWithBatchConsumer_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with
        {
            EnableBatchConsumer = true,
            BatchSize = 0
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("BatchSize must be >= 1");
    }

    [Fact]
    public void Validate_ConsumerBatchTimeoutZeroWithBatchConsumer_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with
        {
            EnableBatchConsumer = true,
            BatchTimeoutMs = 0
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("BatchTimeoutMs must be > 0");
    }

    [Fact]
    public void Validate_ConsumerMaxRetriesZero_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with { MaxRetries = 0 };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxRetries must be >= 1");
    }

    [Fact]
    public void Validate_ConsumerMaxConcurrentHandlersNegative_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with { MaxConcurrentHandlers = -1 };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxConcurrentHandlers must be >= 0");
    }

    [Fact]
    public void Validate_ConsumerRetryDelaysTooShort_Fails()
    {
        var settings = ValidSettings();
        // MaxRetries=4 requires at least 3 delays (for attempts 2, 3, 4), but we only provide 2.
        settings.Consumers[0] = settings.Consumers[0] with
        {
            MaxRetries = 4,
            RetryDelays = [TimeSpan.Zero, TimeSpan.FromSeconds(5)]
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("RetryDelays must have at least 3 entries");
    }

    [Fact]
    public void Validate_ConsumerRetryDelaysWithNegativeValue_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with
        {
            RetryDelays = [TimeSpan.Zero, TimeSpan.FromSeconds(-5)]
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("RetryDelays must not contain negative values");
    }

    [Fact]
    public void Validate_ProducerChannelPoolSizeNegative_Fails()
    {
        var settings = ValidSettings();
        settings.Producers[0] = new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created",
            ChannelPoolSize = -1
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("ChannelPoolSize must be >= 0");
    }

    [Fact]
    public void Validate_ProducerPublishConfirmTimeoutZero_Fails()
    {
        var settings = ValidSettings();
        settings.Producers[0] = new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created",
            PublishConfirmTimeoutMs = 0
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("PublishConfirmTimeoutMs must be > 0");
    }

    /// <summary>
    /// Clones a <see cref="RabbitConnectionOptions"/> applying optional overrides.
    /// </summary>
    /// <remarks>
    /// <see cref="RabbitConnectionOptions"/> is a sealed class with init-only properties,
    /// so it cannot be mutated after construction (and does not support the <c>with</c>
    /// expression, which is only available on records). This helper builds a new instance
    /// in a single object initializer, copying the original's values and applying the
    /// overrides provided via the optional parameters.
    /// </remarks>
    private static RabbitConnectionOptions CloneConnection(
        RabbitConnectionOptions o,
        int? port = null,
        int? requestedHeartbeatSeconds = null,
        int? connectionTimeoutSeconds = null,
        int? initialConnectRetryCount = null,
        int? maxBackoffSeconds = null,
        string? hostName = null)
    {
        return new RabbitConnectionOptions
        {
            Name = o.Name,
            HostName = hostName ?? o.HostName,
            UserName = o.UserName,
            Password = o.Password,
            VirtualHost = o.VirtualHost,
            Port = port ?? o.Port,
            RequestedHeartbeatSeconds = requestedHeartbeatSeconds ?? o.RequestedHeartbeatSeconds,
            ConnectionTimeoutSeconds = connectionTimeoutSeconds ?? o.ConnectionTimeoutSeconds,
            InitialConnectRetryCount = initialConnectRetryCount ?? o.InitialConnectRetryCount,
            MaxBackoffSeconds = maxBackoffSeconds ?? o.MaxBackoffSeconds,
            Tls = o.Tls
        };
    }

    [Fact]
    public void Validate_ConnectionPortOutOfRange_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], port: 0);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Port must be between 1 and 65535");
    }

    [Fact]
    public void Validate_ConnectionPortTooHigh_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], port: 70000);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("Port must be between 1 and 65535");
    }

    [Fact]
    public void Validate_ConnectionHeartbeatZero_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], requestedHeartbeatSeconds: 0);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("RequestedHeartbeatSeconds must be >= 1");
    }

    [Fact]
    public void Validate_ConnectionTimeoutZero_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], connectionTimeoutSeconds: 0);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("ConnectionTimeoutSeconds must be >= 1");
    }

    [Fact]
    public void Validate_ConnectionMaxBackoffZero_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], maxBackoffSeconds: 0);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxBackoffSeconds must be >= 1");
    }

    [Fact]
    public void Validate_ConnectionInitialConnectRetryNegative_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], initialConnectRetryCount: -1);

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("InitialConnectRetryCount must be >= 0");
    }

    [Fact]
    public void Validate_EmptyServiceKey_Fails()
    {
        var settings = ValidSettings();
        settings.Producers[0] = new RabbitProducerOptions
        {
            ServiceKey = "",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created"
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("ServiceKey must be a non-empty, non-whitespace string");
    }

    [Fact]
    public void Validate_WhitespaceHostName_Fails()
    {
        var settings = ValidSettings();
        settings.Connections["main"] = CloneConnection(settings.Connections["main"], hostName: "   ");

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("HostName must be a non-empty, non-whitespace string");
    }

    [Fact]
    public void Validate_EmptyExchangeName_Fails()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with { ExchangeName = "" };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("ExchangeName must be a non-empty, non-whitespace string");
    }

    [Fact]
    public void Validate_BatchConsumerWithValidBatchSettings_Succeeds()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with
        {
            EnableBatchConsumer = true,
            BatchSize = 50,
            BatchTimeoutMs = 3000,
            PrefetchCount = 100
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_ChannelPoolSizeZero_Succeeds()
    {
        var settings = ValidSettings();
        settings.Producers[0] = new RabbitProducerOptions
        {
            ServiceKey = "orders",
            ConnectionName = "main",
            ExchangeName = "orders",
            RoutingKey = "order-created",
            ChannelPoolSize = 0
        };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_MaxConcurrentHandlersZero_Succeeds()
    {
        var settings = ValidSettings();
        settings.Consumers[0] = settings.Consumers[0] with { MaxConcurrentHandlers = 0 };

        var result = _validator.Validate(Options.DefaultName, settings);

        result.Succeeded.Should().BeTrue();
    }
}
