using NUnit.Framework;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.MySql.Tests.Subscriptions
{
    [TestFixture, Category(TestCategory.MySql)]
    public class MySqlSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<MySqlSubscriptionStorageFactory>
    {
    }
}