using NUnit.Framework;
using Rebus.Tests.Contracts.Timeouts;

namespace Rebus.MySql.Tests.Timeouts;

[TestFixture, Category(Categories.MySql)]
public class MySqlBasicStoreAndRetrieveOperations : BasicStoreAndRetrieveOperations<MySqlTimeoutManagerFactory>
{
}