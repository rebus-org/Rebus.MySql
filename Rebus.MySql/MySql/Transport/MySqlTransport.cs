using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Transport;
using Rebus.MySql.Extensions;
using Rebus.Time;
#pragma warning disable 1998

namespace Rebus.MySql.Transport
{
    /// <summary>
    /// Transport which uses MySql in order to receive and send messages.
    /// </summary>
    public class MySqlTransport : ITransport, IInitializable, IDisposable
    {
        private readonly MySqlConnectionHelper _connectionHelper;
        private readonly string _tableName;
        private readonly string _inputQueueName;
        private readonly IAsyncTaskFactory _asyncTaskFactory;
        bool _disposed;
        private readonly ILog _log;

        /// <summary>
        /// The connection key used to connect to the transport database.
        /// </summary>
        const string CurrentConnectionKey = "mysql-transport-current-connection";
        /// <summary>
        /// Header key used to signal the priority of a header.
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();
        /// <summary>
        /// Default interval after which a nessage is considered as expired.
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);
        const int OperationCancelledNumber = 3980;
        private readonly IAsyncTask _expiredMessagesCleanupTask;

        /// <summary>
        /// Interval after which messages are considered expired.
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>
        /// Constructs a MySql transport.
        /// </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName">The name of the table used as the transport.</param>
        /// <param name="inputQueueName">The name of the queue on which messages are received.</param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        public MySqlTransport(MySqlConnectionHelper connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory)
        {
            _connectionHelper = connectionHelper;
            _tableName = tableName;
            _inputQueueName = inputQueueName;
            _asyncTaskFactory = asyncTaskFactory;
            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;
            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup",
                PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
            _log = rebusLoggerFactory.GetLogger<MySqlTransport>();
        }

        /// <summary>
        /// Initializes the transport.
        /// </summary>
        public void Initialize()
        {
            if (_inputQueueName == null) return;
            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>The SQL transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        {
            // NOOP
        }

        /// <summary>
        /// Called when the transport needs to send a message to a specified destination address.
        /// </summary>
        /// <param name="destinationAddress"></param>
        /// <param name="message"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    INSERT INTO `{_tableName}`
                    (
                        `recipient`,
                        `headers`,
                        `body`,
                        `priority`,
                        `visible`,
                        `expiration`,
                        `process_id`
                    )
                    VALUES
                    (
                        @recipient,
                        @headers,
                        @body,
                        @priority,
                        date_add(now(), INTERVAL @visible SECOND),
                        date_add(now(), INTERVAL @ttlseconds SECOND),
                        NULL
                    );";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = GetInitialVisibilityDelay(headers);
                var ttlSeconds = GetTtlSeconds(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add(command.CreateParameter("recipient", DbType.String, destinationAddress));
                command.Parameters.Add(command.CreateParameter("headers", DbType.Binary, serializedHeaders));
                command.Parameters.Add(command.CreateParameter("body", DbType.Binary, message.Body));
                command.Parameters.Add(command.CreateParameter("priority", DbType.Int32, priority));
                command.Parameters.Add(command.CreateParameter("visible", DbType.Int32, initialVisibilityDelay));
                command.Parameters.Add(command.CreateParameter("ttlseconds", DbType.Int32, ttlSeconds));

                await command.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// Called whene the transport needs to process a newly received message.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken))
            {
                var connection = await GetConnection(context);

                TransportMessage receivedTransportMessage;

                using (var selectCommand = connection.CreateCommand())
                {
                    selectCommand.CommandText = $@"
UPDATE {_tableName} SET process_id = @processId WHERE recipient = @recipient AND `visible` < now() AND `expiration` > now() AND process_id IS NULL ORDER BY `priority` ASC, `id` ASC LIMIT 1;
SELECT `id`, `headers`, `body` FROM {_tableName} WHERE process_id = @processId ORDER BY ID LIMIT 1;
DELETE FROM {_tableName} WHERE process_id = @processId;";

                    selectCommand.Parameters.Add(selectCommand.CreateParameter("recipient", DbType.String, _inputQueueName));
                    selectCommand.Parameters.Add(selectCommand.CreateParameter("processId", DbType.Guid, Guid.NewGuid()));

                    try
                    {
                        using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
                        {
                            if (!await reader.ReadAsync(cancellationToken)) return null;

                            var headers = reader["headers"];
                            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
                            var body = (byte[])reader["body"];

                            receivedTransportMessage = new TransportMessage(headersDictionary, body);
                        }
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                }

                return receivedTransportMessage;
            }
        }

        /// <summary>
        /// Returns the address on which the transport is listening.
        /// </summary>
        public string Address => _inputQueueName;

        /// <summary>
        /// Allows the object to be disposed by following the IDisposable pattern.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }

        /// <summary>
        /// Ensures the tables needed for the MySql transport are created when they don't exist yet.
        /// </summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                AsyncHelpers.RunSync(CreateSchema);
            }
            catch (SqlException exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{_tableName}]");
            }
        }

        private async Task CreateSchema()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named '{0}' - will not create anything", _tableName);
                    return;
                }

                _log.Info("Table '{0}' does not exist - it will be created now", _tableName);

                ExecuteCommands(connection, $@"
                    CREATE TABLE {_tableName}
                    (
                        `id` INT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                        `recipient` VARCHAR(200) CHARACTER SET UTF8 NOT NULL,
                        `priority` INT NOT NULL,
                        `expiration` DATETIME NOT NULL,
                        `visible` DATETIME NOT NULL,
                        `headers` MEDIUMBLOB NOT NULL,
                        `body` MEDIUMBLOB NOT NULL,
                        `process_id` CHAR(36) NULL,
                        PRIMARY KEY (`recipient`(128), `priority`, `id`)
                    );
                    ----
                    CREATE INDEX `idx_receive_{_tableName}` ON `{_tableName}`
                    (
                        `recipient`(128) ASC,
                        `priority` ASC,
                        `visible` ASC,
                        `expiration` ASC,
                        `id` ASC
                    );");

                connection.Complete();
            }

        }

        static void ExecuteCommands(MySqlConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (MySqlException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command {command.CommandText}");
            }
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await _connectionHelper.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
                                delete from `{_tableName}`
                                where `recipient` = @recipient
                                and `expiration` < NOW();";
                        command.Parameters.Add(command.CreateParameter("recipient", DbType.String, _inputQueueName));
                        affectedRows = await command.ExecuteNonQueryAsync();
                    }

                    results += affectedRows;
                    connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info(
                    "Performed expired messages cleanup in {0} - {1} expired messages with recipient {2} were deleted",
                    stopwatch.Elapsed, results, _inputQueueName);
            }
        }

        async Task<MySqlConnection> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey, () =>
                    {
                        var dbConnection =  _connectionHelper.GetConnection().Result;
                        context.OnCommitted(async () => dbConnection.Complete());
                        context.OnDisposed(() =>
                        {
                            dbConnection.Dispose();
                        });
                        return dbConnection;
                    });
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        static int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            string deferredUntilDateTimeOffsetString;

            if (!headers.TryGetValue(Headers.DeferredUntil, out deferredUntilDateTimeOffsetString))
            {
                return -1;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return defaultTtlSecondsAbout60Years;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
        }
    }
}