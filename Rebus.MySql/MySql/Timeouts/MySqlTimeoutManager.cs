using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Timeouts;
using Rebus.MySql.Extensions;
using Rebus.Time;
#pragma warning disable 1998

namespace Rebus.MySql.Timeouts
{
    /// <summary>
    /// Stores deferred messages in MySql until the time where it's appropriate to send them.
    /// </summary>
    public class MySqlTimeoutManager : ITimeoutManager
    {
        private readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        private readonly MySqlConnectionHelper _connectionHelper;
        private readonly string _tableName;
        private readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager
        /// </summary>
        public MySqlTimeoutManager(MySqlConnectionHelper connectionHelper, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionHelper == null) throw new ArgumentNullException(nameof(connectionHelper));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper;
            _tableName = tableName;
            _log = rebusLoggerFactory.GetLogger<MySqlTimeoutManager>();
        }

        /// <summary>
        /// Stores the message with the given headers and body data, delaying it until the specified <paramref name="approximateDueTime" />
        /// </summary>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"INSERT INTO `{_tableName}` (`due_time`, `headers`, `body`) VALUES (@due_time, @headers, @body)";

                    command.Parameters.Add(command.CreateParameter("due_time", DbType.DateTime, approximateDueTime.ToUniversalTime().DateTime.AddSeconds(-1)));
                    command.Parameters.Add(command.CreateParameter("headers", DbType.String, _dictionarySerializer.SerializeToString(headers)));
                    command.Parameters.Add(command.CreateParameter("body", DbType.Binary, body));

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Gets due messages as of now, given the approximate due time that they were stored with when <see cref="M:Rebus.Timeouts.ITimeoutManager.Defer(System.DateTimeOffset,System.Collections.Generic.Dictionary{System.String,System.String},System.Byte[])" /> was called
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var connection = await _connectionHelper.GetConnection();

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            SELECT `id`,`headers`,`body`
                            FROM `{_tableName}`
                            WHERE `due_time` <= @current_time
                            ORDER BY `due_time`
                            FOR UPDATE;";
                    command.Parameters.Add(command.CreateParameter("current_time", DbType.DateTime, RebusTime.Now.ToUniversalTime().DateTime));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dueMessages = new List<DueMessage>();

                        while (reader.Read())
                        {
                            var id = (ulong)reader["id"];
                            var headers = _dictionarySerializer.DeserializeFromString((string) reader["headers"]);
                            var body = (byte[]) reader["body"];

                            dueMessages.Add(new DueMessage(headers, body, async () =>
                            {
                                if (connection != null)
                                    using (var deleteCommand = connection.CreateCommand())
                                    {
                                        deleteCommand.CommandText = $@"DELETE FROM `{_tableName}` WHERE `id` = @id";
                                        deleteCommand.Parameters.Add(command.CreateParameter("id", DbType.Int64, id));
                                        await deleteCommand.ExecuteNonQueryAsync();
                                    }
                            }));
                        }

                        return new DueMessagesResult(dueMessages, async () =>
                        {
                            connection.Complete();
                            connection.Dispose();
                        });
                    }
                }
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Checks if the configured timeouts table exists - if it doesn't, it will be created.
        /// </summary>
        public async Task EnsureTableIsCreated()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info("Table '{0}' does not exist - it will be created now", _tableName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            CREATE TABLE `{_tableName}` (
                                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                                `due_time` DATETIME NOT NULL,
                                `headers` TEXT NULL,
                                `body` MEDIUMBLOB NULL,
                                PRIMARY KEY (`id`)
                            );
                            ";

                    command.ExecuteNonQuery();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"CREATE INDEX `idx_{_tableName}` ON `{_tableName}` (`due_time`);";
                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }
    }
}