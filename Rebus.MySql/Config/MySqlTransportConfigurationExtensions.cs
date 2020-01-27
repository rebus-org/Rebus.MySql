using System;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Transport;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class MySqlTransportConfigurationExtension
    {
        /// <summary>
        /// Configures Rebus to use MySql as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseMySql(this StandardConfigurer<ITransport> configurer, string connectionString, string tableName, string inputQueueName)
        {
            Configure(configurer, loggerFactory => new MySqlConnectionHelper(connectionString), tableName, inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use MySql to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UsePostgreSqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, string tableName)
        {
            Configure(configurer, loggerFactory => new MySqlConnectionHelper(connectionString), tableName, null);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        static void Configure(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, MySqlConnectionHelper> connectionProviderFactory, string tableName, string inputQueueName)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
                var connectionProvider = connectionProviderFactory(rebusLoggerFactory);
                var transport = new MySqlTransport(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory);
                transport.EnsureTableIsCreated();
                return transport;
            });

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });
        }
    }
}