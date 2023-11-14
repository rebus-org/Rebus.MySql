using Rebus.Logging;
using Rebus.MySql.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas 
{
    public class MySqlSagaStorageFactory : ISagaStorageFactory
    {
        public MySqlSagaStorageFactory()
        {
            MySqlTestHelper.DropTableIfExists("saga_index");
            MySqlTestHelper.DropTableIfExists("saga_data");
        }

        public ISagaStorage GetSagaStorage()
        {
            var mySqlSagaStorage = new MySqlSagaStorage(MySqlTestHelper.ConnectionHelper, "saga_data", "saga_index", new ConsoleLoggerFactory(false));
            mySqlSagaStorage.EnsureTablesAreCreated().Wait();
            return mySqlSagaStorage;
        }

        public void CleanUp()
        {
            //MySqlTestHelper.DropTableIfExists("saga_index");
            //MySqlTestHelper.DropTableIfExists("saga_data");
        }
    }
}