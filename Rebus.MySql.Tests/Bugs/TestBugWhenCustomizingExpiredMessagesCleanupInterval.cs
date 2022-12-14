using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;

namespace Rebus.MySql.Tests.Bugs;

[TestFixture]
public class TestBugWhenCustomizingExpiredMessagesCleanupInterval : FixtureBase
{
    [Test]
    public async Task CanConfigureIt()
    {
        var activator = Using(new BuiltinHandlerActivator());
        var options = new MySqlTransportOptions(MySqlTestHelper.ConnectionString);

        Configure.With(activator)
            .Transport(t =>
            {
                t.UseMySql(options, TestConfig.GetName("whatever"))
                    .SetExpiredMessagesCleanupInterval(TimeSpan.FromSeconds(5));
            })
            .Start();

        await Task.Delay(TimeSpan.FromSeconds(15));
    }
}