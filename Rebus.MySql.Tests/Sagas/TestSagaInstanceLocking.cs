using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.MySql.ExclusiveLocks;
using Rebus.Routing.TypeBased;
using Rebus.Sagas;
using Rebus.Sagas.Exclusive;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport.InMem;
#pragma warning disable 1998

// ReSharper disable InconsistentNaming

namespace Rebus.MySql.Tests.Sagas;

[TestFixture]
public class TestSagaInstanceLocking : FixtureBase
{
    readonly string _dataTableName = TestConfig.GetName("sagas");
    readonly string _indexTableName = TestConfig.GetName("sagaindex");
    readonly string _lockTableName = TestConfig.GetName("locks");

    protected override void SetUp()
    {
        MySqlTestHelper.DropTable(_indexTableName);
        MySqlTestHelper.DropTable(_dataTableName);
        MySqlTestHelper.DropTable(_lockTableName);
    }

    protected override void TearDown()
    {
        MySqlTestHelper.DropTable(_indexTableName);
        MySqlTestHelper.DropTable(_dataTableName);
        MySqlTestHelper.DropTable(_lockTableName);
    }

    [Test]
    public async Task TestMySqlSagaLocks()
    {
        var loggerFactory = new ListLoggerFactory(outputToConsole: true);
        var network = new InMemNetwork();

        var handlerActivator = Using(new BuiltinHandlerActivator());

        handlerActivator.Handle<ProcessThisThingRequest>((bus, request) => bus.Reply(new ProcessThisThingReply(request.Thing, request.SagaId)));

        Configure.With(handlerActivator)
            .Logging(l => l.None())
            .Transport(t => t.UseInMemoryTransport(network, "processor"))
            .Start();

        var sagaActivator = Using(new BuiltinHandlerActivator());

        sagaActivator.Register((bus, context) => new TypicalContendedSagaExample(bus));

        Configure.With(sagaActivator)
            .Logging(l => l.Use(loggerFactory))
            .Transport(t => t.UseInMemoryTransport(network, "lock-test"))
            .Sagas(s =>
            {
                s.StoreInMySql(MySqlTestHelper.ConnectionString, _dataTableName, _indexTableName);
                s.EnforceExclusiveAccess(c =>
                {
                    var options = new MySqlExclusiveAccessLockOptions(MySqlTestHelper.ConnectionString);
                    var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                    var connectionProvider = options.ConnectionProviderFactory(c);
                    var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                    var rebusTime = c.Get<IRebusTime>();
                    var locker = new MySqlExclusiveAccessLock(connectionProvider, _lockTableName, rebusLoggerFactory, asyncTaskFactory, rebusTime, options);
                    if (options.EnsureTablesAreCreated)
                    {
                        locker.EnsureTableIsCreated();
                    }
                    return locker;
                });
            })
            .Routing(t => t.TypeBased().Map<ProcessThisThingRequest>("processor"))
            .Options(o =>
            {
                o.SetMaxParallelism(100);
                o.SetNumberOfWorkers(10);
            })
            .Start();

        await sagaActivator.Bus.SendLocal(new ProcessTheseThings(Enumerable.Range(0, 10).Select(no => $"THING-{no}")));

        await Task.Delay(TimeSpan.FromSeconds(System.Diagnostics.Debugger.IsAttached ? 30 : 3));

        Assert.That(loggerFactory.Count(l => l.Level >= LogLevel.Warn), Is.EqualTo(0), 
            $@"Didn't expect any logging with level WARNING or above - got this:

{string.Join(Environment.NewLine, loggerFactory)}");
    }

    class ProcessTheseThings
    {
        public ProcessTheseThings(IEnumerable<string> things) => Things = new HashSet<string>(things);

        public HashSet<string> Things { get; }
    }

    class ProcessThisThingRequest
    {
        public ProcessThisThingRequest(string thing, Guid sagaId)
        {
            Thing = thing;
            SagaId = sagaId;
        }

        public string Thing { get; }
        public Guid SagaId { get; }
    }

    class ProcessThisThingReply
    {
        public ProcessThisThingReply(string thing, Guid sagaId)
        {
            Thing = thing;
            SagaId = sagaId;
        }

        public string Thing { get; }
        public Guid SagaId { get; }
    }

    class TypicalContendedSagaExample : Saga<TypicalContendedSagaData>, IAmInitiatedBy<ProcessTheseThings>, IHandleMessages<ProcessThisThingReply>
    {
        readonly IBus _bus;

        public TypicalContendedSagaExample(IBus bus) => _bus = bus ?? throw new ArgumentNullException(nameof(bus));

        protected override void CorrelateMessages(ICorrelationConfig<TypicalContendedSagaData> config)
        {
            config.Correlate<ProcessTheseThings>(m => Guid.NewGuid(), d => d.Id);
            config.Correlate<ProcessThisThingReply>(m => m.SagaId, d => d.Id);
        }

        public async Task Handle(ProcessTheseThings message)
        {
            Data.AddThings(message.Things);

            await Task.WhenAll(message.Things.Select(thing => _bus.Send(new ProcessThisThingRequest(thing, Data.Id))));
        }

        public async Task Handle(ProcessThisThingReply message)
        {
            Data.MarkThisThingAsProcessed(message.Thing);

            if (!Data.HasProcessedAllTheThings()) return;

            MarkAsComplete();
        }
    }

    class TypicalContendedSagaData : SagaData
    {
        public HashSet<string> ThingsToProcess { get; } = new HashSet<string>();

        public bool HasProcessedAllTheThings() => !ThingsToProcess.Any();

        public void AddThings(IEnumerable<string> things)
        {
            foreach (var thing in things)
            {
                ThingsToProcess.Add(thing);
            }
        }

        public void MarkThisThingAsProcessed(string thing)
        {
            ThingsToProcess.Remove(thing);
        }
    }
}
