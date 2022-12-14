using System;
using Rebus.Logging;
using Rebus.MySql.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.MySql.Tests.Timeouts;

public class MySqlTimeoutManagerFactory : ITimeoutManagerFactory
{
    const string TableName = "RebusTimeouts";

    readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

    public MySqlTimeoutManagerFactory()
    {
        MySqlTestHelper.DropAllTables();
    }

    public ITimeoutManager Create()
    {
        var consoleLoggerFactory = new ConsoleLoggerFactory(true);
        var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
        var timeoutManager = new MySqlTimeoutManager(connectionProvider, TableName, consoleLoggerFactory, _fakeRebusTime);

        timeoutManager.EnsureTableIsCreated();

        return timeoutManager;
    }

    public void Cleanup()
    {
        MySqlTestHelper.DropTable(TableName);
    }

    public string GetDebugInfo()
    {
        return "could not provide debug info for this particular timeout manager.... implement if needed :)";
    }

    public void FakeIt(DateTimeOffset fakeTime)
    {
        _fakeRebusTime.SetNow(fakeTime);
    }
}