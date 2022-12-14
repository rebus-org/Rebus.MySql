using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleOther

#pragma warning disable 1998

namespace Rebus.MySql.Tests.Transport;

[TestFixture, Category(Categories.MySql)]
public class TestMySqlTransportReceivePerformance : FixtureBase
{
    const string QueueName = "perftest";

    static readonly string TableName = TestConfig.GetName("perftest");

    protected override void SetUp()
    {
        MySqlTestHelper.DropTable(TableName);
    }

    [TestCase(1000, false, false)]
    [TestCase(1000, true, false)]
    [TestCase(1000, true, true)]
    [TestCase(5000, false, false)]
    public async Task CheckReceivePerformance(int messageCount, bool useOrderingKey, bool useDifferentOrderingKey)
    {
        var adapter = Using(new BuiltinHandlerActivator());

        Configure.With(adapter)
            .Logging(l => l.ColoredConsole(LogLevel.Warn))
            .Transport(t => t.UseMySql(new MySqlTransportOptions(MySqlTestHelper.ConnectionString), QueueName))
            .Options(o =>
            {
                o.SetNumberOfWorkers(0);
                o.SetMaxParallelism(20);
            })
            .Start();

        Console.WriteLine($"Sending {messageCount} messages...");

        var stopwatch = Stopwatch.StartNew();

        // Use the same ordering key for each message to test the performance
        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(i => adapter.Bus.SendLocal($"THIS IS MESSAGE {i}", GetHeaders(useOrderingKey, useDifferentOrderingKey ? i : 0))));

        var elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

        Console.WriteLine($"Inserted {messageCount} messages in {elapsedSeconds:0.0} s - that's {messageCount / elapsedSeconds:0.0} msg/s");

        var counter = Using(new SharedCounter(messageCount));

        adapter.Handle<string>(async message => counter.Decrement());

        Console.WriteLine("Waiting for messages to be received...");

        stopwatch = Stopwatch.StartNew();

        adapter.Bus.Advanced.Workers.SetNumberOfWorkers(3);

        counter.WaitForResetEvent(timeoutSeconds: messageCount / 50 + 5);

        elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

        Console.WriteLine($"{messageCount} messages received in {elapsedSeconds:0.0} s - that's {messageCount / elapsedSeconds:0.0} msg/s");
    }

    private static Dictionary<string, string> GetHeaders(bool useOrderingKey, int counter)
    {
        var headers = useOrderingKey
            ? new Dictionary<string, string>
            {
                {MySqlTransport.OrderingKeyHeaderKey, $"ordering-key-{counter}"}
            }
            : null;
        return headers;
    }
}