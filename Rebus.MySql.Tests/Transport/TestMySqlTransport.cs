using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.MySql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.MySql.Tests.Transport
{
    [TestFixture, Category(Categories.MySql)]
    public class TestMySqlTransport : FixtureBase
    {
        const string QueueName = "input";
        MySqlTransport _transport;
        CancellationToken _cancellationToken;

        protected override void SetUp()
        {
            MySqlTestHelper.DropAllTables();

            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);

            _transport = new MySqlTransport(connectionProvider, QueueName, consoleLoggerFactory, asyncTaskFactory, rebusTime, new MySqlTransportOptions(connectionProvider));
            _transport.EnsureTableIsCreated();

            Using(_transport);

            _transport.Initialize();

            _cancellationToken = new CancellationTokenSource().Token;
        }

        [Test]
        public async Task ReceivesSentMessageWhenTransactionIsCommitted()
        {
            using (var scope = new RebusTransactionScope())
            {
                await _transport.Send(QueueName, RecognizableMessage(), scope.TransactionContext);

                await scope.CompleteAsync();
            }

            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                await scope.CompleteAsync();

                AssertMessageIsRecognized(transportMessage);
            }
        }

        [Test]
        public async Task DoesNotReceiveSentMessageWhenTransactionIsNotCommitted()
        {
            using (var scope = new RebusTransactionScope())
            {
                await _transport.Send(QueueName, RecognizableMessage(), scope.TransactionContext);

                // deliberately skip this:
                //await context.Complete();
            }

            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                Assert.That(transportMessage, Is.Null);
            }
        }

        [Test]
        public async Task IgnoredMessagesWithSameOrderingKeyAsLeasedMessages()
        {
            using (var scope = new RebusTransactionScope())
            {
                // Send three messages, the first two using the same ordering key, one without an ordering key
                // and the last one with a different key
                const string orderingKey = "ordering-key";
                const string differentOrderingKey = "differentOrderingKey";
                await _transport.Send(QueueName, RecognizableMessage(1, orderingKey), scope.TransactionContext);
                await _transport.Send(QueueName, RecognizableMessage(2, orderingKey), scope.TransactionContext);
                await _transport.Send(QueueName, RecognizableMessage(3), scope.TransactionContext);
                await _transport.Send(QueueName, RecognizableMessage(4, differentOrderingKey), scope.TransactionContext);
                await scope.CompleteAsync();
            }

            // We should get message 1, skip 2, then 3 and 4 while inside the transaction
            using (var scope = new RebusTransactionScope())
            {
                var transportMessage1 = await _transport.Receive(scope.TransactionContext, _cancellationToken);
                var transportMessage2 = await _transport.Receive(scope.TransactionContext, _cancellationToken);
                var transportMessage3 = await _transport.Receive(scope.TransactionContext, _cancellationToken);
                var transportMessage4 = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                await scope.CompleteAsync();

                AssertMessageIsRecognized(transportMessage1, 1);
                AssertMessageIsRecognized(transportMessage2, 3);
                AssertMessageIsRecognized(transportMessage3, 4);
                Assert.IsNull(transportMessage4);
            }

            // Now that message one is completed, we should then receive message 2
            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                await scope.CompleteAsync();

                AssertMessageIsRecognized(transportMessage, 2);
            }
        }

        [TestCase(1000)]
        public async Task LotsOfAsyncStuffGoingDown(int numberOfMessages)
        {
            var receivedMessages = 0L;
            var messageIds = new ConcurrentDictionary<int, int>();

            Console.WriteLine("Sending {0} messages", numberOfMessages);

            await Task.WhenAll(Enumerable.Range(0, numberOfMessages)
                .Select(async i =>
                {
                    using var scope = new RebusTransactionScope();
                    await _transport.Send(QueueName, RecognizableMessage(i), scope.TransactionContext);
                    await scope.CompleteAsync();
                    messageIds[i] = 0;
                }));

            Console.WriteLine("Receiving {0} messages", numberOfMessages);

            using (new Timer(_ => Console.WriteLine("Received: {0} msgs", receivedMessages), null, 0, 1000))
            {
                var stopwatch = Stopwatch.StartNew();

                while (Interlocked.Read(ref receivedMessages) < numberOfMessages && stopwatch.Elapsed < TimeSpan.FromMinutes(2))
                {
                    await Task.WhenAll(Enumerable.Range(0, 10).Select(async __ =>
                    {
                        using var scope = new RebusTransactionScope();
                        var msg = await _transport.Receive(scope.TransactionContext, _cancellationToken);
                        await scope.CompleteAsync();

                        if (msg != null)
                        {
                            Interlocked.Increment(ref receivedMessages);
                            var id = int.Parse(msg.Headers["id"]);
                            messageIds.AddOrUpdate(id, 1, (_, existing) => existing + 1);
                        }
                    }));
                }

                await Task.Delay(3000);
            }

            Assert.That(messageIds.Keys.OrderBy(k => k).ToArray(), Is.EqualTo(Enumerable.Range(0, numberOfMessages).ToArray()));

            var kvpsDifferentThanOne = messageIds.Where(kvp => kvp.Value != 1).ToList();

            if (kvpsDifferentThanOne.Any())
            {
                Assert.Fail(@"Oh no! the following IDs were not received exactly once:

{0}",
    string.Join(Environment.NewLine, kvpsDifferentThanOne.Select(kvp => $"   {kvp.Key}: {kvp.Value}")));
            }
        }

        void AssertMessageIsRecognized(TransportMessage transportMessage, int id = 0)
        {
            Assert.That(transportMessage.Headers.GetValue("recognizzle"), Is.EqualTo("hej"));
            Assert.That(transportMessage.Headers.GetValue("id"), Is.EqualTo(id.ToString()));
        }

        static TransportMessage RecognizableMessage(int id = 0, string orderingKey = null)
        {
            var headers = new Dictionary<string, string>
            {
                {"recognizzle", "hej"},
                {"id", id.ToString()}
            };
            if (orderingKey != null)
            {
                headers[MySqlTransport.OrderingKeyHeaderKey] = orderingKey;
            }
            return new TransportMessage(headers, new byte[] { 1, 2, 3 });
        }
    }
}
