using System;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.MySql.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.MySql.Tests.DataBus
{
    public class MySqlDataBusStorageFactory : IDataBusStorageFactory
    {
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public MySqlDataBusStorageFactory()
        {
            MySqlTestHelper.DropTable("databus");
        }

        public IDataBusStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var mySqlDataBusStorage = new MySqlDataBusStorage(connectionProvider, "databus", true, consoleLoggerFactory, _fakeRebusTime, 240);
            mySqlDataBusStorage.Initialize();
            return mySqlDataBusStorage;
        }

        public void CleanUp()
        {
            MySqlTestHelper.DropTable("databus");
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetNow(fakeTime);
        }
    }
}
