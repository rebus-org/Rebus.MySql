using System;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;

namespace Rebus.MySql.Tests.Bugs
{
    [TestFixture]
    public class TestErrorMessageWhenUsingSqlTransportAndRegisteringTimeoutManager
    {
        [Test]
        public void PrintException()
        {
            try
            {
                Configure.With(new BuiltinHandlerActivator())
                    .Transport(t => t.UseMySql(new MySqlTransportOptions(MySqlTestHelper.ConnectionString), "whatever"))
                    .Timeouts(t => t.StoreInMySql(MySqlTestHelper.ConnectionString, "timeouts"))
                    .Start();
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
        }
    }
}
