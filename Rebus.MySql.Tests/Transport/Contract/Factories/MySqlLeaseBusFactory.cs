using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.MySql.Tests.Transport.Contract.Factories
{
    public class MySqlLeaseBusFactory : IBusFactory
    {
        readonly List<IDisposable> _stuffToDispose = new List<IDisposable>();

        public MySqlLeaseBusFactory()
        {
            MySqlTestHelper.DropAllTables();
        }

        public IBus GetBus<TMessage>(string inputQueueAddress, Func<TMessage, Task> handler)
        {
            var builtinHandlerActivator = new BuiltinHandlerActivator();

            builtinHandlerActivator.Handle(handler);

            var tableName = "messages" + TestConfig.Suffix;

            MySqlTestHelper.DropTable(tableName);

            var bus = Configure.With(builtinHandlerActivator)
                .Transport(t => t.UseMySqlInLeaseMode(new MySqlLeaseTransportOptions(MySqlTestHelper.ConnectionString), inputQueueAddress))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(10);
                    o.SetMaxParallelism(10);
                })
                .Start();

            _stuffToDispose.Add(bus);

            return bus;
        }

        public void Cleanup()
        {
            _stuffToDispose.ForEach(d => d.Dispose());
            _stuffToDispose.Clear();
        }
    }
}
