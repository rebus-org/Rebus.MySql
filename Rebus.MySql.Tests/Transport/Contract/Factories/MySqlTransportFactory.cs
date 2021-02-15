using System;
using System.Collections.Generic;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MySql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.MySql.Tests.Transport.Contract.Factories
{
    public class MySqlTransportFactory : ITransportFactory
    {
        readonly HashSet<string> _tablesToDrop = new HashSet<string>();
        readonly List<IDisposable> _disposables = new List<IDisposable>();

        public MySqlTransportFactory()
        {
            MySqlTestHelper.DropAllTables();
        }

        public ITransport CreateOneWayClient()
        {
            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new MySqlTransport(connectionProvider, null, consoleLoggerFactory, asyncTaskFactory, rebusTime, new MySqlTransportOptions(connectionProvider));

            _disposables.Add(transport);

            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var tableName = ("RebusMessages_" + TestConfig.Suffix).TrimEnd('_');

            MySqlTestHelper.DropTable(tableName);

            _tablesToDrop.Add(tableName);

            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new MySqlTransport(connectionProvider, inputQueueAddress, consoleLoggerFactory, asyncTaskFactory, rebusTime, new MySqlTransportOptions(connectionProvider));

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            foreach (var table in _tablesToDrop)
            {
                MySqlTestHelper.DropTable(table);
            }

            _tablesToDrop.Clear();
        }
    }
}
