using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.MySql;
using Rebus.MySql.Sagas;
using Rebus.MySql.Sagas.Serialization;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for sagas
    /// </summary>
    public static class MySqlSagaConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use MySQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaStorage> configurer,
            string connectionString, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
                var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
                var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

                var sagaStorage = new MySqlSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory);
                var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
                var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

                var sagaStorage = new MySqlSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MySQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInMySql(this StandardConfigurer<ISagaStorage> configurer, MySqlSagaStorageOptions options, string dataTableName, string indexTableName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = options.ConnectionProviderFactory(c);
                var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
                var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

                var sagaStorage = new MySqlSagaStorage(
                    connectionProvider: connectionProvider,
                    dataTableName: dataTableName,
                    indexTableName: indexTableName,
                    rebusLoggerFactory: rebusLoggerFactory,
                    sagaTypeNamingStrategy: sagaTypeNamingStrategy,
                    sagaSerializer: serializer
                );

                if (options.EnsureTablesAreCreated)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures saga to use your own custom saga serializer
        /// </summary>
        public static void UseSagaSerializer(this StandardConfigurer<ISagaStorage> configurer, ISagaSerializer serializer = null)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));

            var serializerInstance = serializer ?? new DefaultSagaSerializer();

            configurer.OtherService<ISagaSerializer>().Decorate(c => serializerInstance);
        }

        /// <summary>
        /// Get the registered implementation of <seealso cref="ISagaTypeNamingStrategy"/> or the default <seealso cref="LegacySagaTypeNamingStrategy"/> if one is not configured
        /// </summary>
        static ISagaTypeNamingStrategy GetSagaTypeNamingStrategy(IResolutionContext resolutionContext, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (resolutionContext.Has<ISagaTypeNamingStrategy>())
            {
                return resolutionContext.Get<ISagaTypeNamingStrategy>();
            }

            var logger = rebusLoggerFactory.GetLogger<MySqlSagaStorage>();

            logger.Debug($"An implementation of {nameof(ISagaTypeNamingStrategy)} was not registered. A default, backward compatible, implementation will be used ({nameof(LegacySagaTypeNamingStrategy)}).");

            return new LegacySagaTypeNamingStrategy();
        }
    }
}
