using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas
{
    [TestFixture, Category(TestCategory.MySql)]
    public class SagaIntegrationTests : SagaIntegrationTests<MySqlSagaStorageFactory>
    {
    }
}