using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.MySql;
using Rebus.MySql.Transport;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class MySqlTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use MySQL as its transport. Unlike the <c>UseMySql</c> calls the leased version of the MySQL
        /// transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static MySqlLeaseTransportOptions UseMySqlInLeaseMode(this StandardConfigurer<ITransport> configurer, MySqlLeaseTransportOptions transportOptions, string inputQueueName)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) =>
                    {
                        if (transportOptions.LeasedByFactory == null)
                        {
                            transportOptions.SetLeasedByFactory(() => Environment.MachineName);
                        }

                        return new MySqlLeaseTransport(
                            provider,
                            transportOptions.InputQueueName,
                            context.Get<IRebusLoggerFactory>(),
                            context.Get<IAsyncTaskFactory>(),
                            context.Get<IRebusTime>(),
                            transportOptions.LeaseInterval ?? MySqlLeaseTransport.DefaultLeaseTime,
                            transportOptions.LeaseTolerance ?? MySqlLeaseTransport.DefaultLeaseTolerance,
                            transportOptions.LeasedByFactory,
                            transportOptions

                        );
                    },
                    transportOptions
                )
                .ReadFrom(inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport in "one-way client mode" (i.e. as a send only endpoint). Unlike the <c>UseMySql</c> calls the leased version of the MySQL
        /// transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        public static MySqlLeaseTransportOptions UseMySqlInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, MySqlLeaseTransportOptions transportOptions)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) =>
                    {
                        if (transportOptions.LeasedByFactory == null)
                        {
                            transportOptions.SetLeasedByFactory(() => Environment.MachineName);
                        }

                        return new MySqlLeaseTransport(
                            provider,
                            transportOptions.InputQueueName,
                            context.Get<IRebusLoggerFactory>(),
                            context.Get<IAsyncTaskFactory>(),
                            context.Get<IRebusTime>(),
                            transportOptions.LeaseInterval ?? MySqlLeaseTransport.DefaultLeaseTime,
                            transportOptions.LeaseTolerance ?? MySqlLeaseTransport.DefaultLeaseTolerance,
                            transportOptions.LeasedByFactory,
                            transportOptions
                        );
                    },
                    transportOptions
                )
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static MySqlTransportOptions UseMySql(this StandardConfigurer<ITransport> configurer, MySqlTransportOptions transportOptions, string inputQueueName)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) => new MySqlTransport(provider, inputQueue, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>(), context.Get<IRebusTime>(), transportOptions),
                    transportOptions
                )
                .ReadFrom(inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport in "one-way client mode" (i.e. as a send-only endpoint).
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="transportOptions"></param>
        /// <returns></returns>
        public static MySqlTransportOptions UseMySqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, MySqlTransportOptions transportOptions)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) => new MySqlTransport(provider, inputQueue, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>(), context.Get<IRebusTime>(), transportOptions),
                    transportOptions
                )
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport (in "one-way client mode", i.e. as a send-only endpoint). Unlike the <c>UseMySql</c> calls the leased version of the MySQL
        /// transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an MySqlTransaction and enlist in it</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySqlInLeaseMode) + " with a " + nameof(MySqlLeaseTransportOptions) + " instead.")]
        public static void UseMySqlInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseMySqlInLeaseModeAsOneWayClient(new MySqlLeaseTransportOptions(connectionString, enlistInAmbientTransaction))
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseAutoRenewInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport (in "one-way client mode", i.e. as a send-only endpoint). The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySqlInLeaseMode) + " with a " + nameof(MySqlLeaseTransportOptions) + " instead.")]
        public static void UseMySqlInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            configurer.UseMySqlInLeaseModeAsOneWayClient(new MySqlLeaseTransportOptions(connectionFactory))
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport. Unlike the <c>UseMySql</c> calls the leased version of the MySQL
        /// transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="inputQueueName">Name of the queue, which must be a valid table table in MySQL</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an MySqlTransaction and enlist in it</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySqlInLeaseMode) + " with a " + nameof(MySqlLeaseTransportOptions) + " instead.")]
        public static void UseMySqlInLeaseMode(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseMySqlInLeaseMode(new MySqlLeaseTransportOptions(connectionString, enlistInAmbientTransaction), inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="inputQueueName">Name of the queue, which must be a valid table table in MySQL</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="MySqlLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="MySqlLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySqlInLeaseMode) + " with a " + nameof(MySqlLeaseTransportOptions) + " instead.")]
        public static void UseMySqlInLeaseMode(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool ensureTablesAreCreated = true)
        {
            configurer.UseMySqlInLeaseMode(new MySqlLeaseTransportOptions(connectionFactory), inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use MySQL to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySql) + " with a " + nameof(MySqlTransport) + " instead.")]
        public static void UseMySqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory)
        {
            configurer.UseMySqlAsOneWayClient(new MySqlTransportOptions(connectionFactory));
        }

        /// <summary>
        /// Configures Rebus to use MySQL to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySql) + " with a " + nameof(MySqlTransport) + " instead.")]
        public static void UseMySqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, bool enlistInAmbientTransaction = false)
        {
            configurer.UseMySqlAsOneWayClient(new MySqlTransportOptions(connectionString, enlistInAmbientTransaction))
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySql) + " with a " + nameof(MySqlTransport) + " instead.")]
        public static void UseMySql(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string inputQueueName, bool ensureTablesAreCreated = true)
        {
            configurer.UseMySql(new MySqlTransportOptions(connectionFactory), inputQueueName)
                .ReadFrom(inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated);
        }

        /// <summary>
        /// Configures Rebus to use MySQL as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseMySql) + " with a " + nameof(MySqlTransport) + " instead.")]
        public static void UseMySql(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseMySql(new MySqlTransportOptions(connectionString, enlistInAmbientTransaction), inputQueueName)
                .ReadFrom(inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated);
        }

        delegate MySqlTransport TransportFactoryDelegate(IResolutionContext context, IDbConnectionProvider connectionProvider, string inputQueueName);

        static TTransportOptions Configure<TTransportOptions>(StandardConfigurer<ITransport> configurer, TransportFactoryDelegate transportFactory, TTransportOptions transportOptions) where TTransportOptions : MySqlTransportOptions
        {
            configurer.Register(context =>
                {
                    if (transportOptions.IsOneWayQueue)
                    {
                        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
                    }

                    var connectionProvider = transportOptions.ConnectionProviderFactory(context);
                    var transport = transportFactory(context, connectionProvider, transportOptions.InputQueueName);
                    if ((transportOptions.InputQueueName != null) && (transportOptions.EnsureTablesAreCreated == true))
                    {
                        transport.EnsureTableIsCreated();
                    }

                    return transport;
                }
            );

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager(),
                @"A timeout manager cannot be explicitly configured when using MySQL as the
transport. This is because because the SQL transport has built-in deferred 
message capabilities, and therefore it is not necessary to configure anything 
else to be able to delay message delivery.");

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });

            configurer.OtherService<Options>().Decorate(c =>
            {
                var options = c.Get<Options>();

                if (string.IsNullOrWhiteSpace(options.ExternalTimeoutManagerAddressOrNull))
                {
                    options.ExternalTimeoutManagerAddressOrNull = MySqlTransport.MagicExternalTimeoutManagerAddress;
                }

                return options;
            });

            return transportOptions;
        }
    }
}
