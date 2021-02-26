using System;
using Rebus.MySql.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Provides extensions for managing <seealso cref="MySqlTransportOptions"/>
    /// </summary>
    public static class MySqlTransportOptionsExtensions
    {
        /// <summary>
        /// Flags the transport as only being used for sending
        /// </summary>
        public static TTransportOptions AsOneWayClient<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlTransportOptions
        {
            options.InputQueueName = null;
            return options;
        }

        /// <summary>
        /// Configures the transport to read from <paramref name="inputQueueName"/>
        /// </summary>
        public static TTransportOptions ReadFrom<TTransportOptions>(this TTransportOptions options, string inputQueueName) where TTransportOptions : MySqlTransportOptions
        {
            options.InputQueueName = inputQueueName;
            return options;
        }

        /// <summary>
        /// Opts the client out of any table creation
        /// </summary>
        public static TTransportOptions OptOutOfTableCreation<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlTransportOptions
        {
            options.EnsureTablesAreCreated = false;
            return options;
        }

        /// <summary>
        /// Sets if table creation is allowed
        /// </summary>
        public static TTransportOptions SetEnsureTablesAreCreated<TTransportOptions>(this TTransportOptions options, bool ensureTablesAreCreated) where TTransportOptions : MySqlTransportOptions
        {
            options.EnsureTablesAreCreated = ensureTablesAreCreated;
            return options;
        }

        /// <summary>
        /// Sets if table will be dropped automatically
        /// </summary>
        public static TTransportOptions SetAutoDeleteQueue<TTransportOptions>(this TTransportOptions options, bool autoDeleteQueue) where TTransportOptions : MySqlTransportOptions
        {
            options.AutoDeleteQueue = autoDeleteQueue;
            return options;
        }

        /// <summary>
        /// Sets the maximum amount of time an unprocessed message can remain unacknowledged before it is replayed. Defaults to 10 seconds.
        /// </summary>
        public static TTransportOptions SetMessageAckTimeout<TTransportOptions>(this TTransportOptions options, TimeSpan messageAckTimeout) where TTransportOptions : MySqlTransportOptions
        {
            options.MessageAckTimeout = messageAckTimeout;
            return options;
        }

        /// <summary>
        /// Sets the delay between executions of the background cleanup task
        /// </summary>
        public static TTransportOptions SetExpiredMessagesCleanupInterval<TTransportOptions>(this TTransportOptions options, TimeSpan interval) where TTransportOptions : MySqlTransportOptions
        {
            options.ExpiredMessagesCleanupInterval = interval;
            return options;
        }

        /// <summary>
        /// If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
        /// </summary>
        public static TLeaseTransportOptions SetLeaseInterval<TLeaseTransportOptions>(this TLeaseTransportOptions options, TimeSpan? leaseInterval) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.LeaseInterval = leaseInterval;
            return options;
        }

        /// <summary>
        /// If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
        /// </summary>
        public static TLeaseTransportOptions SetLeaseTolerance<TLeaseTransportOptions>(this TLeaseTransportOptions options, TimeSpan? leaseTolerance) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.LeaseTolerance = leaseTolerance;
            return options;
        }

        /// <summary>
        /// Enables or disables automatic lease renewal. If <paramref name="automaticallyRenewLeases"/> is <c>true</c> and <paramref name="automaticLeaseRenewInterval"/> is <c>null</c> it will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/>
        /// </summary>
        public static TLeaseTransportOptions SetAutomaticLeaseRenewal<TLeaseTransportOptions>(this TLeaseTransportOptions options, bool automaticallyRenewLeases, TimeSpan? automaticLeaseRenewInterval) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.AutomaticallyRenewLeases = automaticallyRenewLeases;
            options.LeaseAutoRenewInterval = automaticLeaseRenewInterval;
            return options;
        }


        /// <summary>
        /// Enables automatic lease renewal. If <paramref name="automaticLeaseRenewInterval"/> is <c>null</c> then <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/> will be used instead
        /// </summary>
        public static TLeaseTransportOptions EnableAutomaticLeaseRenewal<TLeaseTransportOptions>(this TLeaseTransportOptions options, TimeSpan? automaticLeaseRenewInterval) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.AutomaticallyRenewLeases = true;
            options.LeaseAutoRenewInterval = automaticLeaseRenewInterval;
            return options;
        }

        /// <summary>
        /// Disables automatic lease renewal. Message handlers that run longer than <seealso cref="MySqlLeaseTransportOptions.LeaseInterval"/> would be processed by another worker even if the worker processing this message is healthy
        /// </summary>
        public static TLeaseTransportOptions DisableAutomaticLeaseRenewal<TLeaseTransportOptions>(this TLeaseTransportOptions options) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.AutomaticallyRenewLeases = false;
            options.LeaseAutoRenewInterval = null;
            return options;
        }

        /// <summary>
        /// If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used
        /// </summary>
        public static TLeaseTransportOptions SetLeasedByFactory<TLeaseTransportOptions>(this TLeaseTransportOptions options, Func<string> leasedByFactory) where TLeaseTransportOptions : MySqlLeaseTransportOptions
        {
            options.LeasedByFactory = leasedByFactory;
            return options;
        }
    }
}
