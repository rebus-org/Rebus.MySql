using Rebus.Logging;
using Rebus.MySql.Subscriptions;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.MySql.Tests.Subscriptions
{
    public class MySqlSubscriptionStorageFactory : ISubscriptionStorageFactory
    {
        public MySqlSubscriptionStorageFactory()
        {
            Cleanup();
        }

        public ISubscriptionStorage Create()
        {
            var subscriptionStorage = new MySqlSubscriptionStorage(MySqlTestHelper.ConnectionHelper, "subscriptions", true, new ConsoleLoggerFactory(false));
            subscriptionStorage.EnsureTableIsCreated();
            return subscriptionStorage;
        }

        public void Cleanup()
        {
            MySqlTestHelper.DropTableIfExists("subscriptions");
        }
    }
}