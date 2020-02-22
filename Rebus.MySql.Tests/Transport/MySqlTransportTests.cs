using System;
using System.Collections.Generic;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.MySql.Tests.Timeouts;
using Rebus.MySql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.MySql.Tests.Transport
{
    public class MySqlTransportFactory : ITransportFactory
    {

         readonly HashSet<string> _tablesToDrop = new HashSet<string>();
        readonly List<IDisposable> _disposables = new List<IDisposable>();


        [TestFixture, Category(Categories.MySql)]
        public class MySqlTransportBasicSendReceive : BasicSendReceive<MySqlTransportFactory> { }

        [TestFixture, Category(Categories.MySql)]
        public class MySqlTransportMessageExpiration : MessageExpiration<MySqlTransportFactory> { }


        public ITransport CreateOneWayClient()
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');
             _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new MySqlConnectionHelper(MySqlTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new MySqlTransport(connectionHelper, tableName, null, consoleLoggerFactory, asyncTaskFactory, new FakeRebusTime());

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');

            _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new MySqlConnectionHelper(MySqlTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new MySqlTransport(connectionHelper, tableName, inputQueueAddress, consoleLoggerFactory, asyncTaskFactory, new FakeRebusTime());

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            foreach (var tableToDrop in _tablesToDrop)
            {
                MySqlTestHelper.DropTableIfExists(tableToDrop);
            }

            _tablesToDrop.Clear();
        }
    }
}