using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Subscriptions;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.MySql.Tests.Subscriptions
{
    public class MySqlSubscriptionStorageFactory : ISubscriptionStorageFactory
    {
        const string TableName = "RebusSubscriptions";

        public MySqlSubscriptionStorageFactory()
        {
            MySqlTestHelper.DropAllTables();
        }

        public ISubscriptionStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var storage = new MySqlSubscriptionStorage(connectionProvider, TableName, true, consoleLoggerFactory);

            storage.EnsureTableIsCreated();

            return storage;
        }

        public void Cleanup()
        {
            MySqlTestHelper.DropTable(TableName);
        }
    }
}
