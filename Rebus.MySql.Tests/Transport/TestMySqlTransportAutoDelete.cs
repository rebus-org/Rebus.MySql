using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.Tests.Contracts;

namespace Rebus.MySql.Tests.Transport
{
	[TestFixture, Category(Categories.MySql)]
	public class TestMySqlTransportAutoDelete : FixtureBase
    {
        protected override void SetUp()
        {
            MySqlTestHelper.DropAllTables();
        }

        [Test]
        public async Task Dispose_WhenAutoDeleteQueueEnabled_DropsInputQueue()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);

            const string queueName = "input";

            var options = new MySqlTransportOptions(MySqlTestHelper.ConnectionString);

            var activator = Using(new BuiltinHandlerActivator());
            Configure.With(activator)
                .Logging(l => l.Use(consoleLoggerFactory))
                .Transport(t => t.UseMySql(options, queueName).SetAutoDeleteQueue(true))
                .Start();

            using (var connection = await connectionProvider.GetConnection())
            {
                Assert.That(connection.GetTableNames().Contains(TableName.Parse(queueName)), Is.True);
            }

            CleanUpDisposables();

            using (var connection = await connectionProvider.GetConnection())
            {
                Assert.That(connection.GetTableNames().Contains(TableName.Parse(queueName)), Is.False);
            }
        }
    }
}
