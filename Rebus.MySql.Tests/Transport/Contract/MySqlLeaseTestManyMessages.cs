using NUnit.Framework;
using Rebus.MySql.Tests.Transport.Contract.Factories;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.MySql.Tests.Transport.Contract
{
    [TestFixture]
    public class MySqlLeaseTestManyMessages : TestManyMessages<MySqlLeaseBusFactory> { }
}
