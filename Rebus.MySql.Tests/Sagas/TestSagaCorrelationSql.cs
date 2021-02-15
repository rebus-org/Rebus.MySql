using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas
{
    [TestFixture]
    public class TestSagaCorrelationSql : TestSagaCorrelation<MySqlSagaStorageFactory> { }
}