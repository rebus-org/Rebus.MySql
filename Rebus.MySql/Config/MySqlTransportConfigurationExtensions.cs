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
        /// <returns>Transport options so they can be configured</returns>
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
        /// <returns>Transport options so they can be configured</returns>
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
        /// <returns>Transport options so they can be configured</returns>
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
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <returns>Transport options so they can be configured</returns>
        public static MySqlTransportOptions UseMySqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, MySqlTransportOptions transportOptions)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) => new MySqlTransport(provider, inputQueue, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>(), context.Get<IRebusTime>(), transportOptions),
                    transportOptions
                )
                .AsOneWayClient();
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
