using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas;

[TestFixture]
public class MySqlSagaSnapshotStorageTest : SagaSnapshotStorageTest<MySqlSnapshotStorageFactory>
{
}