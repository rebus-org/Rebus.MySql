using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Subscriptions;

namespace Rebus.MySql.Subscriptions;

/// <summary>
/// Implementation of <see cref="ISubscriptionStorage"/> that persists subscriptions in a table in MySQL
/// </summary>
public class MySqlSubscriptionStorage : ISubscriptionStorage
{
    readonly IDbConnectionProvider _connectionProvider;
    readonly TableName _tableName;
    readonly ILog _log;

    // Default column sizes to use when creating the table
    int _defaultTopicLength = 200;
    int _defaultAddressLength = 200;

    /// <summary>
    /// Constructs the storage using the specified connection provider and table to store its subscriptions. If the subscription
    /// storage is shared by all subscribers and publishers, the <paramref name="isCentralized"/> parameter can be set to true
    /// in order to subscribe/unsubscribe directly instead of sending subscription/unsubscription requests
    /// </summary>
    public MySqlSubscriptionStorage(IDbConnectionProvider connectionProvider, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
    {
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

        IsCentralized = isCentralized;

        _log = rebusLoggerFactory.GetLogger<MySqlSubscriptionStorage>();
        _tableName = TableName.Parse(tableName);
    }

    /// <summary>
    /// Reading the lengths of the [topic] and [address] columns from MySQL and store them locally
    /// </summary>
    void GetColumnWidths()
    {
        try
        {
            using (var connection = _connectionProvider.GetConnection())
            {
                _topicLength = GetColumnWidth("topic", connection);
                _addressLength = GetColumnWidth("address", connection);
            }
        }
        catch (Exception exception)
        {
            throw new RebusApplicationException(exception, "Error during schema reflection");
        }
    }

    int GetColumnWidth(string columnName, IDbConnection connection)
    {
        // Use the current database prefix if one is not provided
        var schema = _tableName.Schema;
        if (string.IsNullOrWhiteSpace(schema))
        {
            schema = connection.Database;
        }
        var sql = $@"
                SELECT CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND 
                      TABLE_NAME = '{_tableName.Name}' AND
                      COLUMN_NAME = '{columnName}'";
        try
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;
                return Convert.ToInt32(command.ExecuteScalar());
            }
        }
        catch (Exception exception)
        {
            throw new RebusApplicationException(exception, $"Could not get size of the [{columnName}] column from {_tableName} - executed SQL: '{sql}'");
        }
    }

    /// <summary>
    /// Returns the topic length if initialized, and reads from the database if not. We read this when
    /// we first need it so we do not try to hit MySQL during startup in case it is down.
    /// </summary>
    int TopicLength
    {
        get
        {
            if (_topicLength == null)
            {
                GetColumnWidths();
            }
            return _topicLength.Value;
        }
    }
    int? _topicLength;

    /// <summary>
    /// Returns the address length if initialized, and reads from the database if not. We read this when
    /// we first need it so we do not try to hit MySQL during startup in case it is down.
    /// </summary>
    int AddressLength
    {
        get
        {
            if (_addressLength == null)
            {
                GetColumnWidths();
            }
            return _addressLength.Value;
        }
    }
    int? _addressLength;

    /// <summary>
    /// Creates the subscriptions table if necessary
    /// </summary>
    public void EnsureTableIsCreated()
    {
        try
        {
            InnerEnsureTableIsCreated();
        }
        catch
        {
            // if it failed because of a collision between another thread doing the same thing, just try again once:
            InnerEnsureTableIsCreated();
        }
    }

    void InnerEnsureTableIsCreated()
    {
        using (var connection = _connectionProvider.GetConnection())
        {
            var tableNames = connection.GetTableNames();
            if (tableNames.Contains(_tableName))
            {
                return;
            }

            _log.Info("Table {tableName} does not exist - it will be created now", _tableName.QualifiedName);

            using (var command = connection.CreateCommand())
            {
                connection.ExecuteCommands($@"
                        CREATE TABLE {_tableName.QualifiedName} (
                            `topic` VARCHAR({_defaultTopicLength}) NOT NULL,
	                        `address` VARCHAR({_defaultAddressLength}) NOT NULL,
                            PRIMARY KEY (`topic`, `address`)
                        )");
                command.ExecuteNonQuery();
            }
            connection.Complete();
        }
    }

    /// <summary>
    /// Gets all destination addresses for the given topic
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        using (var connection = await _connectionProvider.GetConnectionAsync())
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                        SELECT address 
                        FROM {_tableName.QualifiedName} 
                        WHERE topic = @topic";
                command.Parameters.Add("topic", MySqlDbType.VarChar, TopicLength).Value = topic;
                var subscriberAddresses = new List<string>();
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var address = (string)reader["address"];
                        subscriberAddresses.Add(address);
                    }
                }
                return subscriberAddresses.ToArray();
            }
        }
    }

    /// <summary>
    /// Registers the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
    /// </summary>
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        CheckLengths(topic, subscriberAddress);

        using (var connection = await _connectionProvider.GetConnectionAsync())
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                        INSERT IGNORE INTO {_tableName.QualifiedName} (
                            topic,
                            address
                        ) VALUES (
                            @topic, 
                            @address
                        )";
                command.Parameters.Add("topic", MySqlDbType.VarChar, TopicLength).Value = topic;
                command.Parameters.Add("address", MySqlDbType.VarChar, AddressLength).Value = subscriberAddress;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            await connection.CompleteAsync().ConfigureAwait(false);
        }
    }

    void CheckLengths(string topic, string subscriberAddress)
    {
        if (topic.Length > TopicLength)
        {
            throw new ArgumentException(
                $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the topic is greater than {TopicLength} (which is the current MAX length allowed by the current {_tableName} schema)");
        }

        if (subscriberAddress.Length > AddressLength)
        {
            throw new ArgumentException(
                $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the subscriber address is greater than {AddressLength} (which is the current MAX length allowed by the current {_tableName} schema)");
        }
    }

    /// <summary>
    /// Unregisters the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
    /// </summary>
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        CheckLengths(topic, subscriberAddress);

        using (var connection = await _connectionProvider.GetConnectionAsync())
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"DELETE FROM {_tableName.QualifiedName} WHERE topic = @topic AND address = @address";
                command.Parameters.Add("topic", MySqlDbType.VarChar, TopicLength).Value = topic;
                command.Parameters.Add("address", MySqlDbType.VarChar, AddressLength).Value = subscriberAddress;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            await connection.CompleteAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Gets whether this subscription storage is centralized
    /// </summary>
    public bool IsCentralized { get; }
}
