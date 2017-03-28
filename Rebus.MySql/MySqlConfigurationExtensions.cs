using Rebus.Auditing.Sagas;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql.Sagas;
using Rebus.MySql.Subscriptions;
using Rebus.MySql.Timeouts;
using Rebus.Sagas;
using Rebus.Subscriptions;
using Rebus.Timeouts;

namespace Rebus.MySql
{
    /// <summary>
    /// Configuration extensions for MySql persistence
    /// </summary>
    public static class MySqlConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use MySQL to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISubscriptionStorage> configurer,
            string connectionString, string tableName, bool isCentralized = false,
            bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionHelper = new MySqlConnectionHelper(connectionString);
                var subscriptionStorage = new MySqlSubscriptionStorage(
                    connectionHelper, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated().RunSynchronously();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySQL to store timeouts.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ITimeoutManager> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var subscriptionStorage = new MySqlTimeoutManager(new MySqlConnectionHelper(connectionString), tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    AsyncHelpers.RunSync(() => subscriptionStorage.EnsureTableIsCreated());
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaStorage> configurer,
            string connectionString, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var sagaStorage = new MySqlSagaStorage(new MySqlConnectionHelper(connectionString), dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    AsyncHelpers.RunSync(() => sagaStorage.EnsureTablesAreCreated());
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySQL to store saga data snapshots, using the specified table to store the data
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaSnapshotStorage> configurer,
            string connectionString, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var sagaStorage = new MySqlSagaSnapshotStorage(new MySqlConnectionHelper(connectionString), tableName);

                if (automaticallyCreateTables)
                {
                    AsyncHelpers.RunSync(() => sagaStorage.EnsureTableIsCreated());
                }

                return sagaStorage;
            });
        }
    }
}