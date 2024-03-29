﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.MySql.Transport;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Tests.Contracts.Utilities;
#pragma warning disable 1998

namespace Rebus.MySql.Tests.Transport;

[TestFixture]
public class TestMessagePriority : FixtureBase
{
    protected override void SetUp() => MySqlTestHelper.DropAllTables();

    [TestCase(20)]
    public async Task ReceivedMessagesByPriority_HigherIsMoreImportant(int messageCount)
    {
        var counter = new SharedCounter(messageCount);
        var receivedMessagePriorities = new List<int>();
        var server = new BuiltinHandlerActivator();

        server.Handle<string>(async str =>
        {
            Console.WriteLine($"Received message: {str}");
            var parts = str.Split(' ');
            var priority = int.Parse(parts[1]);
            receivedMessagePriorities.Add(priority);
            counter.Decrement();
        });

        var serverBus = Configure.With(Using(server))
            .Transport(t => t.UseMySql(new MySqlTransportOptions(MySqlTestHelper.ConnectionString), "server"))
            .Options(o =>
            {
                o.SetNumberOfWorkers(0);
                o.SetMaxParallelism(1);
            })
            .Start();

        var clientBus = Configure.With(Using(new BuiltinHandlerActivator()))
            .Transport(t => t.UseMySqlAsOneWayClient(new MySqlTransportOptions(MySqlTestHelper.ConnectionString)))
            .Routing(t => t.TypeBased().Map<string>("server"))
            .Start();

        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .InRandomOrder()
            .Select(priority => SendPriMsg(clientBus, priority)));

        serverBus.Advanced.Workers.SetNumberOfWorkers(1);

        counter.WaitForResetEvent();

        await Task.Delay(TimeSpan.FromSeconds(1));

        Assert.That(receivedMessagePriorities.Count, Is.EqualTo(messageCount));
        Assert.That(receivedMessagePriorities.ToArray(), Is.EqualTo(Enumerable.Range(0, messageCount).Reverse().ToArray()));
    }

    static Task SendPriMsg(IBus clientBus, int priority) => clientBus.Send($"prioritet {priority}", new Dictionary<string, string>
    {
        {MySqlTransport.MessagePriorityHeaderKey, priority.ToString()}
    });
}