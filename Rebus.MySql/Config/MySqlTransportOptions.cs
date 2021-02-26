using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Transport;

namespace Rebus.Config
{
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
        /// Set to the maximum amount of time an unprocessed message can remain unacknowledged before it is replayed. Defaults to 10 seconds.
        /// </summary>
        public TimeSpan MessageAckTimeout  { get; internal set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Gets the delay between executions of the background cleanup task
        /// </summary>
        internal TimeSpan? ExpiredMessagesCleanupInterval { get; set; }
    }
}
