using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.MySql.Tests.Transport
{
    [TestFixture]
    public class TestMySqlTransportAckExpiration : FixtureBase
    {
        BuiltinHandlerActivator _activator;
        ListLoggerFactory _loggerFactory;
        IBusStarter _starter;

        protected override void SetUp()
        {
            MySqlTestHelper.DropAllTables();

            var queueName = TestConfig.GetName("ack_message_timeout");

            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            _loggerFactory = new ListLoggerFactory(outputToConsole: true);

            // Use an ACK timeout of 2 seconds
            var ackTimeout = TimeSpan.FromSeconds(2);

            // Force the ACK timeout to a longer value for this test, as we specifically make the handlers take a long time
            _starter = Configure.With(_activator)
                .Logging(l => l.Use(_loggerFactory))
                .Transport(t =>
                {
                    var options = new MySqlTransportOptions(MySqlTestHelper.ConnectionString)
                        .SetMessageAckTimeout(ackTimeout)
                        .SetExpiredMessagesCleanupInterval(ackTimeout);
                    t.UseMySql(options, queueName);
                })
                .Create();
        }

        [Test]
        public void MessageReplaysIfNotAcknowledgedInTime()
        {
            var doneHandlingMessage = new ManualResetEvent(false);

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            string firstMessageId = null;
            string secondMessageId = null;
            _activator.Handle<string>(async str =>
            {
                var messageId = MessageContext.Current.Message.Headers[Headers.MessageId];
                if (firstMessageId == null)
                {
                    // If this is the first message, wait 3 seconds and bail if we get canceled
                    firstMessageId = messageId;
                    Console.WriteLine($"First message {messageId}, gonna wait too long!");
                    await Task.Delay(TimeSpan.FromSeconds(3));
                    return;
                }

                Console.WriteLine($"Replayed message {messageId}, ending test");
                secondMessageId = messageId;
                doneHandlingMessage.Set();
            });

            var bus = _starter.Start();
            bus.SendLocal("hello my good friend!").Wait();

            doneHandlingMessage.WaitOrDie(TimeSpan.FromMinutes(2));
            stopwatch.Stop();
            var elapsedTime = stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"DONE: took time:{elapsedTime}ms");

            // Make sure the second message was a replayed message and did not run too quickly. It must be at
            // least 2s for the timeout to have worked correctly.
            Assert.AreEqual(firstMessageId, secondMessageId);
            Assert.IsTrue(elapsedTime > 2000);
        }
    }
}
