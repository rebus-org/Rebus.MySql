using NUnit.Framework;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.MySql.Tests.DataBus
{
    [TestFixture]
    public class MySqlDataBusStorageTest : GeneralDataBusStorageTests<MySqlDataBusStorageFactory> { }
}
