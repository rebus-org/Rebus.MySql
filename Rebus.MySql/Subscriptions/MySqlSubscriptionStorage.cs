using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.MySql.Extensions;

namespace Rebus.MySql.Subscriptions
{
    /// <summary>
    /// Stores subscriptions in MySql.
    /// </summary>
    public class MySqlSubscriptionStorage : ISubscriptionStorage
    {
        const int DuplicateKeyViolation = 1062;

        private readonly MySqlConnectionHelper _connectionHelper;

        private readonly string _tableName;

        private readonly ILog _log;

        /// <summary>
        /// Constructs the subscription storage, storing subscriptions in the specified <paramref name="tableName"/>.
        /// If <paramref name="isCentralized"/> is true, subscribing/unsubscribing will be short-circuited by manipulating
        /// subscriptions directly, instead of requesting via messages
        /// </summary>
        public MySqlSubscriptionStorage(MySqlConnectionHelper connectionHelper, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
        {
            _connectionHelper = connectionHelper;
            _tableName = tableName;
            IsCentralized = isCentralized;
            _log = rebusLoggerFactory.GetLogger<MySqlSubscriptionStorage>();
        }

        /// <summary>
        /// Gets all destination addresses for the given topic
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"select `address` from `{_tableName}` where `topic` = @topic";
                command.Parameters.Add(command.CreateParameter("topic", DbType.String, topic));

                var endpoints = new List<string>();
                
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        endpoints.Add((string)reader["address"]);
                    }
                }

                return endpoints.ToArray();
            }
        }

        /// <summary>
        /// Registers the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"insert into `{_tableName}` (`topic`, `address`) values (@topic, @address)";
                command.Parameters.Add(command.CreateParameter("topic", DbType.String, topic));
                command.Parameters.Add(command.CreateParameter("address", DbType.String, subscriberAddress));

                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (MySqlException exception) when (exception.Number == DuplicateKeyViolation)
                {
                    // it's already there
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Unregisters the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"delete from `{_tableName}` where `topic` = @topic and `address` = @address;";

                command.Parameters.Add(command.CreateParameter("topic", DbType.String, topic));
                command.Parameters.Add(command.CreateParameter("address", DbType.String, subscriberAddress));

                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (MySqlException exception)
                {
                    Console.WriteLine(exception);
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Gets whether the subscription storage is centralized and thus supports bypassing the usual subscription request
        /// (in a fully distributed architecture, a subscription is established by sending a <see cref="T:Rebus.Messages.Control.SubscribeRequest" />
        /// to the owner of a given topic, who then remembers the subscriber somehow - if the subscription storage is
        /// centralized, the message exchange can be bypassed, and the subscription can be established directly by
        /// having the subscriber register itself)
        /// </summary>
        public bool IsCentralized { get; }

        /// <summary>
        /// Creates the subscriptions table if no table with the specified name exists
        /// </summary>
        public async Task EnsureTableIsCreated()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames().ToHashSet();

                if (tableNames.Contains(_tableName)) return;

                _log.Info($"Table '{_tableName}' does not exist - it will be created now");

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            CREATE TABLE `{_tableName}` (
                                `topic` VARCHAR(200) NOT NULL,
                                `address` VARCHAR(200) NOT NULL,
                                PRIMARY KEY (`topic`, `address`)
                            );";
                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }
    }
}