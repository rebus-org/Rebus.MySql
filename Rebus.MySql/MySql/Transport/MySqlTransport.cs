using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.MySql.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses MySQL to do its thing
    /// </summary>
    public class MySqlTransport : ITransport, IInitializable, IDisposable
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        /// <summary>
        /// When a message is sent to this address, it will be deferred into the future!
        /// </summary>
        public const string MagicExternalTimeoutManagerAddress = "##### MagicExternalTimeoutManagerAddress #####";

        /// <summary>
        /// Special message priority header that can be used with the <see cref="MySqlTransport"/>. The value must be an <see cref="Int32"/>
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Default delay between executing the background cleanup task
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        /// <summary>
        /// Connection provider for obtaining a database connection
        /// </summary>
        protected readonly IDbConnectionProvider _connectionProvider;

        private readonly IRebusTime _rebusTime;

        /// <summary>
        /// Name of the table this transport is using for storage
        /// </summary>
        protected readonly TableName _receiveTableName;

        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILog _log;

        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly TimeSpan _messageAckTimeout;
        readonly bool _autoDeleteQueue;
        bool _disposed;

        /// <summary>
        /// Constructs the transport with the given <see cref="IDbConnectionProvider"/>
        /// </summary>
        public MySqlTransport(IDbConnectionProvider connectionProvider, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime, MySqlTransportOptions options)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _receiveTableName = inputQueueName != null ? TableName.Parse(inputQueueName) : null;

            _log = rebusLoggerFactory.GetLogger<MySqlTransport>();

            var cleanupInterval = options.ExpiredMessagesCleanupInterval ?? DefaultExpiredMessagesCleanupInterval;
            var intervalSeconds = (int)cleanupInterval.TotalSeconds;

            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: intervalSeconds);
            _autoDeleteQueue = options.AutoDeleteQueue;
            _messageAckTimeout = options.MessageAckTimeout;
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the SQL table
        /// </summary>
        public void Initialize()
        {
            if (_receiveTableName == null) return;

            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// Gets the name that this SQL transport will use to query by when checking the messages table
        /// </summary>
        public string Address => _receiveTableName?.QualifiedName;

        /// <summary>
        /// Creates the table named after the given <paramref name="address"/>
        /// </summary>
        public void CreateQueue(string address)
        {
            if (address == null) return;

            var tableName = TableName.Parse(address);

            EnsureTableIsCreated(tableName);
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureTableIsCreated()
        {
            EnsureTableIsCreated(_receiveTableName);
        }

        void EnsureTableIsCreated(TableName table)
        {
            try
            {
                InnerEnsureTableIsCreated(table);
            }
            catch (Exception)
            {
                // if it fails the first time, and if it's because of some kind of conflict,
                // we should run it again and see if the situation has stabilized
                InnerEnsureTableIsCreated(table);
            }
        }

        void InnerEnsureTableIsCreated(TableName table)
        {
            using (var connection = _connectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();
                if (tableNames.Contains(table))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", table.QualifiedName);
                }
                else
                {
                    _log.Info("Table {tableName} does not exist - it will be created now", table.QualifiedName);

                    connection.ExecuteCommands($@"
                        CREATE TABLE {table.QualifiedName} (
                            `id` BIGINT NOT NULL AUTO_INCREMENT,
                            `priority` INT NOT NULL,
                            `expiration` DATETIME(6) NOT NULL,
                            `visible` DATETIME(6) NOT NULL,
                            `headers` LONGBLOB NOT NULL,
                            `body` LONGBLOB NOT NULL,
                            `processing` TINYINT(1) NULL,
                            PRIMARY KEY (`id`)
                        );
                        ----
                        CREATE INDEX `idx_receive` ON {table.QualifiedName} (
                            `priority` DESC,
                            `visible` ASC,
                            `id` ASC,
                            `expiration` ASC,
                            `processing` ASC
                        );
                        ----
                        CREATE INDEX `idx_expiration` ON {table.QualifiedName} (
                            `expiration`,
                            `processing`
                        );");
                }

                AdditionalSchemaModifications(connection, table);
                connection.Complete();
            }
        }

        /// <summary>
        /// Provides an opportunity for derived implementations to also update the schema
        /// </summary>
        /// <param name="connection">Connection to the database</param>
        /// <param name="table">Name of the table to create schema modifications for</param>
        protected virtual void AdditionalSchemaModifications(IDbConnection connection, TableName table)
        {
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if it is, it will be dropped
        /// </summary>
        void EnsureTableIsDropped()
        {
            try
            {
                InnerEnsureTableIsDropped(_receiveTableName);
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                InnerEnsureTableIsDropped(_receiveTableName);
            }
        }

        void InnerEnsureTableIsDropped(TableName table)
        {
            using (var connection = _connectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (!tableNames.Contains(table))
                {
                    _log.Info("A table named {tableName} doesn't exist", table.QualifiedName);
                    return;
                }

                _log.Info("Table {tableName} exists - it will be dropped now", table.QualifiedName);

                connection.ExecuteCommands($"DROP TABLE IF EXISTS {table};");
                AdditionalSchemaModificationsOnDeleteQueue(connection, table);
                connection.Complete();
            }
        }

        /// <summary>
        /// Provides an opportunity for derived implementations to also update the schema when the queue is deleted automatically
        /// </summary>
        protected void AdditionalSchemaModificationsOnDeleteQueue(IDbConnection connection, TableName table)
        {
        }

        /// <summary>
        /// Sends the given transport message to the specified destination queue address by adding it to the queue's table.
        /// </summary>
        public virtual async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context).ConfigureAwait(false);

            var destinationAddressToUse = GetDestinationAddressToUse(destinationAddress, message);
            try
            {
                await InnerSend(destinationAddressToUse, message, connection).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new RebusApplicationException(e, $"Unable to send to destination {destinationAddress}");
            }
        }

        /// <summary>
        /// Receives the next message by querying the input queue table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        /// <param name="context">Transaction context the receive is operating on</param>
        /// <param name="cancellationToken">Cancellation token for the receive operation</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        public virtual Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            // NOTE: This function is specifically NOT implemented as async, for performance reasons. Performance
            // testing has shown that it's actually slower to run this operation async than it is to run it without
            // the async await operations.
            using (var connection = _connectionProvider.GetConnection())
            {
                while (true)
                {
                    try
                    {
                        TransportMessage transportMessage;

                        using (var command = connection.CreateCommand())
                        {
                            // Read the message and extra the data and ID if found
                            var tableName = _receiveTableName.QualifiedName;
                            command.CommandText = $@"
                                SELECT id,
                                       headers,
                                       body
                                FROM {tableName}
                                WHERE visible < now(6) AND 
                                      expiration > now(6) AND
                                      processing = 0 
                                ORDER BY priority DESC, 
                                         visible ASC, 
                                         id ASC 
                                LIMIT 1
                                FOR UPDATE";
                            long messageId;
                            using (var reader = command.ExecuteReader())
                            {
                                transportMessage = ExtractTransportMessageFromReader(reader);
                                if (transportMessage == null) return Task.FromResult<TransportMessage>(null);
                                messageId = (long)reader["id"];
                            }

                            // Mark the message as being processed within the transaction
                            command.CommandText = $@"
                                UPDATE {tableName} 
                                SET processing = 1 
                                WHERE id = @message_id";
                            command.Parameters.Add("message_id", MySqlDbType.Int64).Value = messageId;
                            command.ExecuteNonQuery();

                            // Now apply transaction semantics to clear the message later
                            ApplyTransactionSemantics(context, messageId);
                        }
                        connection.Complete();
                        return Task.FromResult(transportMessage);
                    }
                    catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.LockDeadlock)
                    {
                        // If we get a transaction deadlock here, keep trying until we succeed
                    }
                }
            }
        }

        /// <summary>
        /// Maps a <seealso cref="MySqlDataReader"/> that's read a result from the message table into a <seealso cref="TransportMessage"/>
        /// </summary>
        /// <returns>A <seealso cref="TransportMessage"/> representing the row or <c>null</c> if no row was available</returns>
        protected static TransportMessage ExtractTransportMessageFromReader(MySqlDataReader reader)
        {
            if (reader.Read() == false)
            {
                return null;
            }
            var headers = reader["headers"];
            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
            var body = (byte[])reader["body"];
            return new TransportMessage(headersDictionary, body);
        }

        /// <summary>
        /// Responsible for releasing the lease on message failure and removing the message on transaction commit
        /// </summary>
        /// <param name="context">Transaction context of the message processing</param>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        private void ApplyTransactionSemantics(ITransactionContext context, long messageId)
        {
            context.OnAborted(
                ctx =>
                {
                    try
                    {
                        ClearProcessing(messageId);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "While Resetting Lease");
                    }
                }
            );

            context.OnCommitted(
                ctx =>
                {
                    try
                    {
                        DeleteMessage(messageId);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "While Deleting Message");
                    }

                    return Task.CompletedTask;
                }
            );
        }

        /// <summary>
        /// Responsible for clearing the processing flag on a message on transaction abort
        /// </summary>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        private void ClearProcessing(long messageId)
        {
            using (var connection = _connectionProvider.GetConnection())
            {
                while (true)
                {
                    try
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = $@"UPDATE {_receiveTableName.QualifiedName} SET processing = 0 WHERE id = {messageId}";
                            command.ExecuteNonQuery();
                        }
                        connection.Complete();
                        return;
                    }
                    catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.LockDeadlock)
                    {
                        // Keep trying if we get a deadlock until it succeeds
                    }
                }
            }
        }

        /// <summary>
        /// Responsible for deleting the message on transaction commit
        /// </summary>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        protected void DeleteMessage(long messageId)
        {
            using (var connection = _connectionProvider.GetConnection())
            {
                while (true)
                {
                    try
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = $@"DELETE FROM {_receiveTableName.QualifiedName} WHERE id = {messageId}";
                            command.ExecuteNonQuery();
                        }
                        connection.Complete();
                        return;
                    }
                    catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.LockDeadlock)
                    {
                        // Keep trying if we get a deadlock until it succeeds
                    }
                }
            }
        }

        /// <summary>
        /// Gets the address a message will actually be sent to. Handles deferred messages.
        /// </summary>
        protected static string GetDestinationAddressToUse(string destinationAddress, TransportMessage message)
        {
            return string.Equals(destinationAddress, MagicExternalTimeoutManagerAddress, StringComparison.CurrentCultureIgnoreCase)
                ? GetDeferredRecipient(message)
                : destinationAddress;
        }

        static string GetDeferredRecipient(TransportMessage message)
        {
            if (message.Headers.TryGetValue(Headers.DeferredRecipient, out var destination))
            {
                return destination;
            }

            throw new InvalidOperationException($"Attempted to defer message, but no '{Headers.DeferredRecipient}' header was on the message");
        }

        /// <summary>
        /// Performs persistence of a message to the underlying table
        /// </summary>
        /// <param name="destinationAddress">Address the message will be sent to</param>
        /// <param name="message">Message to be sent</param>
        /// <param name="connection">Connection to use for writing to the database</param>
        protected async Task InnerSend(string destinationAddress, TransportMessage message, IDbConnection connection)
        {
            var sendTable = TableName.Parse(destinationAddress);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    INSERT INTO {sendTable.QualifiedName} (
                        `headers`,
                        `body`,
                        `priority`,
                        `visible`,
                        `expiration`,
                        `processing`
                    ) VALUES (
                        @headers,
                        @body,
                        @priority,
                        date_add(date_add(now(6), INTERVAL @visible_total_seconds SECOND), INTERVAL @visible_microseconds MICROSECOND),
                        date_add(date_add(now(6), INTERVAL @ttl_total_seconds SECOND), INTERVAL @ttl_microseconds MICROSECOND),
                        0
                    );";
                var headers = message.Headers.Clone();
                var priority = GetMessagePriority(headers);
                var visible = GetInitialVisibilityDelay(headers);
                var ttl = GetTtl(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add("headers", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(serializedHeaders.Length)).Value = serializedHeaders;
                command.Parameters.Add("body", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(message.Body.Length)).Value = message.Body;
                command.Parameters.Add("priority", MySqlDbType.Int32).Value = priority;
                command.Parameters.Add("visible_total_seconds", MySqlDbType.Int32).Value = (int)visible.TotalSeconds;
                command.Parameters.Add("visible_microseconds", MySqlDbType.Int32).Value = visible.Milliseconds * 1000;
                command.Parameters.Add("ttl_total_seconds", MySqlDbType.Int32).Value = (int)ttl.TotalSeconds;
                command.Parameters.Add("ttl_microseconds", MySqlDbType.Int32).Value = ttl.Milliseconds * 1000;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        TimeSpan GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return TimeSpan.Zero;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            var visibilityDelay = deferredUntilTime - _rebusTime.Now;
            return visibilityDelay;
        }

        static TimeSpan GetTtl(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
            {
                return TimeSpan.FromSeconds(defaultTtlSecondsAbout60Years);
            }

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return timeToBeReceived;
        }

        /// <summary>
        /// Task called periodically to clean up expired messages if they got suck for a long time
        /// </summary>
        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
                {
                    // First get a batch of up to 100 messages at a time to delete in one go. To avoid deadlocks
                    // with running messages that are wrapped up in transactions, we need to select the ID's out
                    // of the table that need to be deleted, so we can delete specifically by ID to avoid deadlocking
                    // on the entire table. If we try to do something like delete from blah where expiration < now()
                    // that will take a lock on the entire table which will stall out until the messages are processed.
                    int affectedRows = 0;
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $@"
                            SELECT id
                            FROM {_receiveTableName.QualifiedName}
                            WHERE expiration < now() and
                                  processing = 0
                            LIMIT 100";
                        var messageIds = new List<long>();
                        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            while (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                messageIds.Add((long)reader["id"]);
                            }
                        }

                        // If we got any messages to delete, clean them up in a single delete statement
                        if (messageIds.Count > 0)
                        {
                            command.CommandText = $"DELETE FROM {_receiveTableName.QualifiedName} WHERE id in ({string.Join(",", messageIds)})";
                            affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }

                        // Check for any dead messages and release them again. This is any message where the visibility is now
                        // older than our message ACK timeout.
                        command.CommandText = $@"
                            SELECT id
                            FROM {_receiveTableName.QualifiedName}
                            WHERE visible < date_sub(now(6), INTERVAL @message_timeout_seconds SECOND) and
                                  processing = 1
                            LIMIT 100";
                        command.Parameters.Add("message_timeout_seconds", MySqlDbType.Int32).Value = (int)_messageAckTimeout.TotalSeconds;
                        messageIds.Clear();
                        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            while (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                messageIds.Add((long)reader["id"]);
                            }
                        }

                        // If we got any messages to delete, clean them up in a single delete statement
                        if (messageIds.Count > 0)
                        {
                            command.CommandText = $"UPDATE {_receiveTableName.QualifiedName} SET processing = 0 WHERE id in ({string.Join(",", messageIds)})";
                            affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }

                    // Commit all the changes
                    results += affectedRows;
                    await connection.CompleteAsync().ConfigureAwait(false);

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info("Performed expired messages cleanup in {cleanupTimeSeconds} - {expiredMessageCount} expired messages with recipient {queueName} were deleted",
                    stopwatch.Elapsed.TotalSeconds, results, _receiveTableName.QualifiedName);
            }
        }

        async Task<IDbConnection> GetConnection(ITransactionContext context)
        {
            // Get the connection and set it up to be disposed or committed in the transaction context. MySQL cannot
            // share connections, so we do not store it in the shared context items, but create a new one each time.
            var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false);
            context.OnCommitted(async ctx => await connection.CompleteAsync().ConfigureAwait(false));
            context.OnDisposed(ctx => connection.Dispose());
            return connection;
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

        /// <summary>
        /// Shuts down the background timer
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
                if (_autoDeleteQueue)
                    EnsureTableIsDropped();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
