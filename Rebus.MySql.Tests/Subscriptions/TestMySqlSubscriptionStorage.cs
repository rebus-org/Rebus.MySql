using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.MySql.Tests.Subscriptions.This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.MySql;
using Rebus.MySql.Subscriptions;
using Rebus.Tests.Contracts;
#pragma warning disable 1998

namespace Rebus.MySql.Tests.Subscriptions
{
    [TestFixture]
    public class TestMySqlSubscriptionStorage : FixtureBase
    {
        [Test]
        public async Task GetsAnAppropriateExceptionWhenAttemptingToRegisterSubscriberForTooLongTopic()
        {
            const string subscriberAddress = "subscriberino";

            var storage = GetStorage(false);

            var tooLongTopic = typeof(SomeClass).GetSimpleAssemblyQualifiedName();

            Console.WriteLine($@"The topic is pretty long:

{tooLongTopic}

");

            var aggregateException = Assert.Throws<AggregateException>(() =>
            {
                storage.RegisterSubscriber(tooLongTopic, subscriberAddress).Wait();
            });

            var baseException = aggregateException.GetBaseException();

            Console.WriteLine($@"Here is the exception:

{baseException}

");
        }

        [Test]
        [Description("Creates a subscribers table where the space for topic and subscriber address is distributed to reserve more space for the topic (which is probably best)")]
        public async Task WorksWithCustomSchema()
        {
            const string subscriberAddress = "subscriberino";

            var storage = GetStorage(true);

            var topic1 = typeof(SomeClass).GetSimpleAssemblyQualifiedName();
            var topic2 = typeof(AnotherClass).GetSimpleAssemblyQualifiedName();

            await storage.RegisterSubscriber(topic1, subscriberAddress);

            var subscribers1 = await storage.GetSubscriberAddresses(topic1);
            var subscribers2 = await storage.GetSubscriberAddresses(topic2);

            Assert.That(subscribers1, Is.EqualTo(new[] { subscriberAddress }));
            Assert.That(subscribers2, Is.EqualTo(new string[0]));
        }

        static MySqlSubscriptionStorage GetStorage(bool createCustomSchema)
        {
            MySqlTestHelper.DropTable("Subscriptions");

            var loggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(MySqlTestHelper.ConnectionString, loggerFactory);
            var storage = new MySqlSubscriptionStorage(connectionProvider, "Subscriptions", true, loggerFactory);

            if (createCustomSchema)
            {
                var tableName = TableName.Parse("Subscriptions");

                MySqlTestHelper.Execute($@"
                    CREATE TABLE {tableName.QualifiedName} (
                        `topic` VARCHAR(350) NOT NULL,
	                    `address` VARCHAR(50) NOT NULL,
                        PRIMARY KEY (`topic`, `address`)
                    )");
            }
            else
            {
                storage.EnsureTableIsCreated();
            }

            storage.Initialize();

            return storage;
        }
    }

    namespace This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.MySql
    {
        public class SomeClass
        {
        }

        public class AnotherClass
        {
        }
    }
}
