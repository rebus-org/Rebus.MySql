using NUnit.Framework;
using Rebus.Logging;
using Rebus.MySql.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.MySql.Tests.Timeouts
{
    [TestFixture, Category(TestCategory.MySql)]
    public class TestMySqlTimeoutManager : BasicStoreAndRetrieveOperations<MySqlTimeoutManagerFactory>
    {
    }

    public class MySqlTimeoutManagerFactory : ITimeoutManagerFactory
    {
        public MySqlTimeoutManagerFactory()
        {
            //MySqlTestHelper.DropTableIfExists("timeouts");
        }

        public ITimeoutManager Create()
        {
            var timeoutManager = new MySqlTimeoutManager(MySqlTestHelper.ConnectionHelper, "timeouts", new ConsoleLoggerFactory(false));
            timeoutManager.EnsureTableIsCreated();
            return timeoutManager;
        }

        public void Cleanup()
        {
            MySqlTestHelper.DropTableIfExists("timeouts");
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }
    }

}