using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Time;
using Rebus.Timeouts;
// ReSharper disable AccessToDisposedClosure

namespace Rebus.MySql.Timeouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that uses MySQL to store messages until it's time to deliver them.
    /// </summary>
    public class MySqlTimeoutManager : ITimeoutManager
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();
        readonly IDbConnectionProvider _connectionProvider;
        private readonly IRebusTime _rebusTime;
        readonly TableName _tableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager, using the specified connection provider and table to store the messages until they're due.
        /// </summary>
        public MySqlTimeoutManager(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));

            _tableName = TableName.Parse(tableName);
            _log = rebusLoggerFactory.GetLogger<MySqlTimeoutManager>();
        }

        /// <summary>
        /// Creates the due messages table if necessary
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

                connection.ExecuteCommands($@"
                    CREATE TABLE {_tableName.QualifiedName} (
                        `id` BIGINT NOT NULL AUTO_INCREMENT,
                        `due_time` DATETIME(6) NOT NULL,
                        `headers` LONGTEXT NOT NULL,
                        `body` LONGBLOB NOT NULL,
                        PRIMARY KEY (`id`)
                    );
                    ----
                    CREATE INDEX `idx_due_time` ON {_tableName.QualifiedName} (
                        `due_time`
                    );");
                connection.Complete();
            }
        }

        /// <summary>
        /// Defers the message to the time specified by <paramref name="approximateDueTime"/> at which point in time the message will be
        /// returned to whoever calls <see cref="GetDueMessages"/>
        /// </summary>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        INSERT INTO {_tableName.QualifiedName} (
                            `due_time`,
                            `headers`,
                            `body`
                        ) VALUES (
                            @due_time,
                            @headers,
                            @body
                        );";
                    var headersString = HeaderSerializer.SerializeToString(headers);
                    command.Parameters.Add("due_time", MySqlDbType.DateTime).Value = approximateDueTime;
                    command.Parameters.Add("headers", MySqlDbType.VarChar, MathUtil.GetNextPowerOfTwo(headersString.Length)).Value = headersString;
                    command.Parameters.Add("body", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(body.Length)).Value = body;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                await connection.CompleteAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Gets messages due for delivery at the current time
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false);
            try
            {
                var dueMessages = new List<DueMessage>();

                const int maxDueTimeouts = 1000;

                using (var command = connection.CreateCommand())
                {
                    var tableName = _tableName.QualifiedName;
                    command.CommandText = $@"
                        SELECT id,
                               headers,
                               body
                        FROM {tableName}
                        WHERE due_time <= @current_time 
                        ORDER BY due_time ASC
                        FOR UPDATE";
                    command.Parameters.Add("current_time", MySqlDbType.DateTime).Value = _rebusTime.Now;

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var id = (long)reader["id"];
                            var headersString = (string)reader["headers"];
                            var headers = HeaderSerializer.DeserializeFromString(headersString);
                            var body = (byte[])reader["body"];

                            var sqlTimeout = new DueMessage(headers, body, async () =>
                            {
                                using (var deleteCommand = connection.CreateCommand())
                                {
                                    deleteCommand.CommandText = $"DELETE FROM {tableName} WHERE id = {id}";
                                    await deleteCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                                }
                            });

                            dueMessages.Add(sqlTimeout);

                            if (dueMessages.Count >= maxDueTimeouts) break;
                        }
                    }

                    return new DueMessagesResult(dueMessages, async () =>
                    {
                        using (connection)
                        {
                            await connection.CompleteAsync().ConfigureAwait(false);
                        }
                    });
                }
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }
    }
}
