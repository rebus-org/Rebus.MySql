using NUnit.Framework;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Sagas;
using Rebus.MySql.Sagas.Serialization;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas
{
    [TestFixture, Category(Categories.MySql)]
    public class MySqlSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<MySqlSagaStorageFactory> { }

    [TestFixture, Category(Categories.MySql)]
    public class MySqlSagaStorageConcurrencyHandling : ConcurrencyHandling<MySqlSagaStorageFactory> { }

    [TestFixture, Category(Categories.MySql)]
    public class MySqlSagaStorageSagaIntegrationTests : SagaIntegrationTests<MySqlSagaStorageFactory> { }

    public class MySqlSagaStorageFactory : ISagaStorageFactory
    {
        const string IndexTableName = "RebusSagaIndex";
        const string DataTableName = "RebusSagaData";

        public MySqlSagaStorageFactory()
        {
            CleanUp();
        }

        public ISagaStorage GetSagaStorage()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var sagaTypeNamingStrategy = new LegacySagaTypeNamingStrategy();
            var serializer = new DefaultSagaSerializer();
            var storage = new MySqlSagaStorage(connectionProvider, DataTableName, IndexTableName, consoleLoggerFactory, sagaTypeNamingStrategy, serializer);

            storage.EnsureTablesAreCreated();

            return storage;
        }

        public void CleanUp()
        {
            MySqlTestHelper.DropTable(IndexTableName);
            MySqlTestHelper.DropTable(DataTableName);
        }
    }
}
