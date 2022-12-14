using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql.ExclusiveLocks;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;

namespace Rebus.MySql.Tests.ExclusiveLocks;

[TestFixture]
public class TestMySqlExclusiveAccessLock : FixtureBase
{
    readonly string _lockTableName = TestConfig.GetName("locks");

    protected override void SetUp()
    {
        MySqlTestHelper.DropTable(_lockTableName);
    }

    protected override void TearDown()
    {
        MySqlTestHelper.DropTable(_lockTableName);
    }

    [Test]
    public async Task TestLockFunctions()
    {
        var rebusTime = new DefaultRebusTime();
        var consoleLoggerFactory = new ConsoleLoggerFactory(false);
        var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
        var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);

        // Create a MySQL locker
        var locker = new MySqlExclusiveAccessLock(connectionProvider, _lockTableName, consoleLoggerFactory, asyncTaskFactory, rebusTime, new MySqlExclusiveAccessLockOptions(connectionProvider));
        locker.EnsureTableIsCreated();

        // Try the check function which will return false initially
        const string lockName = "my_lock";
        var result = await locker.IsLockAcquiredAsync(lockName, CancellationToken.None);
        Assert.That(result, Is.False);

        // Get the lock once
        result = await locker.AcquireLockAsync(lockName, CancellationToken.None);
        Assert.That(result, Is.True);

        // Try to get it again, and it should fail
        result = await locker.AcquireLockAsync(lockName, CancellationToken.None);
        Assert.That(result, Is.False);

        // Try the check function
        result = await locker.IsLockAcquiredAsync(lockName, CancellationToken.None);
        Assert.That(result, Is.True);

        // Now release the lock
        await locker.ReleaseLockAsync(lockName);

        // Try the check function and it should be false now
        result = await locker.IsLockAcquiredAsync(lockName, CancellationToken.None);
        Assert.That(result, Is.False);
    }
}