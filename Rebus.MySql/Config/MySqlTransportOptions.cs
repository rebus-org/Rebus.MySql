using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Transport;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure the <seealso cref="MySqlTransport"/>
/// </summary>
public class MySqlTransportOptions : MySqlOptions
{
    /// <summary>
    /// Create an instance of the transport with a pre-created <seealso cref="DbConnectionProvider"/>
    /// </summary>
    public MySqlTransportOptions(IDbConnectionProvider connectionProvider)
    {
        ConnectionProviderFactory = (resolutionContext) => connectionProvider;
    }

    /// <summary>
    /// Create an instance of the transport with a <paramref name="connectionProviderFactory"/> that can use the <see cref="IResolutionContext"/> to look up things
    /// </summary>
    public MySqlTransportOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
    {
        ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
    }

    /// <summary>
    /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
    /// </summary>
    public MySqlTransportOptions(string connectionString, bool enlistInAmbientTransaction = false)
    {
        ConnectionProviderFactory = resolutionContext => new DbConnectionProvider(connectionString, resolutionContext.Get<IRebusLoggerFactory>(), enlistInAmbientTransaction);
    }

    /// <summary>
    /// Creates an instance of the transport with utilising an <seealso cref="IDbConnectionProvider"/> factory
    /// </summary>
    public MySqlTransportOptions(Func<Task<IDbConnection>> connectionFactory)
    {
        ConnectionProviderFactory = resolutionContext => new DbConnectionFactoryProvider(connectionFactory);
    }

    /// <summary>
    /// Name of the input queue to process. If <c>null</c> or whitespace the transport will be configured in one way mode (send only)
    /// </summary>
    public string InputQueueName { get; internal set; }

    /// <summary>
    /// If <c>true</c> the transport is configured in one way mode
    /// </summary>
    public bool IsOneWayQueue => InputQueueName == null;

    /// <summary>
    /// If true, the input queue table will be automatically dropped on transport disposal
    /// </summary>
    public bool AutoDeleteQueue { get; internal set; } = false;

    /// <summary>
    /// Gets the delay between executions of the background cleanup task
    /// </summary>
    internal TimeSpan? ExpiredMessagesCleanupInterval { get; set; }

    /// <summary>
    /// If <c>null</c> will default to <seealso cref="MySqlTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
    /// </summary>
    public TimeSpan? LeaseInterval { get; internal set; }

    /// <summary>
    /// If <c>null</c> will default to <seealso cref="MySqlTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
    /// </summary>
    public TimeSpan? LeaseTolerance { get; internal set; }

    /// <summary>
    /// If not <c>null</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This value Specifies how frequently
    /// a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB
    /// communication. A value 50% of <seealso cref="LeaseInterval"/> should be appropriate.
    /// </summary>
    public TimeSpan? LeaseAutoRenewInterval { get; internal set; }

    /// <summary>
    /// If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used
    /// </summary>
    public Func<string> LeasedByFactory { get; internal set; }
}