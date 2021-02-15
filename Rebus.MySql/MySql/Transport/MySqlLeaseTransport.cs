using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.MySql.Transport
{
    /// <summary>
    /// Similar to <seealso cref="MySqlTransport"/> but does not maintain an active connection during message processing. Instead a "lease" is acquired for each message and only once "committed" is the message removed from the queue.
    /// <remarks>Note: This also changes the semantics of sending. Sent messages are queued in memory and are not committed to memory until the sender has committed</remarks>
    /// </summary>
    public class MySqlLeaseTransport : MySqlTransport
    {
        /// <summary>
        /// Key for storing the outbound message buffer when performing <seealso cref="Send"/>
        /// </summary>
        public const string OutboundMessageBufferKey = "sql-server-transport-leased-outbound-message-buffer";

        /// <summary>
        /// Size of the leased_by column
        /// </summary>
        public const int LeasedByColumnSize = 200;

        /// <summary>
        /// If not specified the default time messages are leased for
        /// </summary>
        public static readonly TimeSpan DefaultLeaseTime = TimeSpan.FromMinutes(5);

        /// <summary>
        /// If not specified the amount of tolerance workers will allow a message which has already been leased
        /// </summary>
        public static readonly TimeSpan DefaultLeaseTolerance = TimeSpan.FromSeconds(30);

        /// <summary>
        /// If not specified the amount of time the workers will automatically renew leases for actively handled messages
        /// </summary>
        public static readonly TimeSpan DefaultLeaseAutomaticRenewal = TimeSpan.FromSeconds(150);

        readonly TimeSpan _leaseInterval;
        readonly TimeSpan _leaseTolerance;
        readonly bool _automaticLeaseRenewal;
        readonly TimeSpan _automaticLeaseRenewalInterval;
        readonly Func<string> _leasedByFactory;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionProvider">A <see cref="IDbConnection"/> to obtain a database connection</param>
        /// <param name="inputQueueName">Name of the queue this transport is servicing</param>
        /// <param name="rebusLoggerFactory">A <seealso cref="IRebusLoggerFactory"/> for building loggers</param>
        /// <param name="asyncTaskFactory">A <seealso cref="IAsyncTaskFactory"/> for creating periodic tasks</param>
        /// <param name="rebusTime">A <seealso cref="IRebusTime"/> to provide the current time</param>
        /// <param name="leaseInterval">Interval of time messages are leased for</param>
        /// <param name="leaseTolerance">Buffer to allow lease overruns by</param>
        /// <param name="leasedByFactory">Factory for generating a string which identifies who has leased a message (eg. A hostname)</param>
        /// <param name="options">Additional options</param>
        public MySqlLeaseTransport(
            IDbConnectionProvider connectionProvider,
            string inputQueueName,
            IRebusLoggerFactory rebusLoggerFactory,
            IAsyncTaskFactory asyncTaskFactory,
            IRebusTime rebusTime,
            TimeSpan leaseInterval,
            TimeSpan? leaseTolerance,
            Func<string> leasedByFactory,
            MySqlLeaseTransportOptions options
            ) : base(connectionProvider, inputQueueName, rebusLoggerFactory, asyncTaskFactory, rebusTime, options)
        {
            _leasedByFactory = leasedByFactory;
            _leaseInterval = leaseInterval;
            _leaseTolerance = leaseTolerance ?? TimeSpan.FromSeconds(15);

            var automaticLeaseRenewalInterval = options.LeaseAutoRenewInterval;

            if (!automaticLeaseRenewalInterval.HasValue)
            {
                _automaticLeaseRenewal = false;
            }
            else
            {
                _automaticLeaseRenewal = true;
                _automaticLeaseRenewalInterval = automaticLeaseRenewalInterval.Value;
            }
        }

        /// <summary>
        /// Sends the given transport message to the specified logical destination address by adding it to the messages table.
        /// </summary>
        public override Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var outboundMessageBuffer = GetOutboundMessageBuffer(context);

            outboundMessageBuffer.Enqueue(
                new AddressedTransportMessage
                {
                    DestinationAddress = GetDestinationAddressToUse(destinationAddress, message),
                    Message = message
                }
            );

            return Task.CompletedTask;
        }

        /// <summary>
        /// Provides an opportunity for derived implementations to also update the schema
        /// </summary>
        /// <param name="connection">Connection to the database</param>
        /// <param name="table">Name of the table to create schema modifications for</param>
        protected override void AdditionalSchemaModifications(IDbConnection connection, TableName table)
        {
            // Use the current database prefix if one is not provided
            var schema = table.Schema;
            if (string.IsNullOrWhiteSpace(schema))
            {
                schema = connection.Database;
            }
            var tableName = table.Name;

            // If any of our columns do not exist, run the schema upgrade
            var columns = connection.GetColumns(schema, tableName);
            var indexes = connection.GetIndexes(schema, tableName);
            if (!columns.ContainsKey("leased_until") ||
                !columns.ContainsKey("leased_by") ||
                !columns.ContainsKey("leased_at") ||
                !indexes.ContainsKey("idx_receive_lease"))
            {
                connection.ExecuteCommands($@"
                    {MySqlMagic.CreateColumnIfNotExistsSql(schema, tableName, "leased_until", "datetime(6) null")}
                    ----
                    {MySqlMagic.CreateColumnIfNotExistsSql(schema, tableName, "leased_by", $"varchar({LeasedByColumnSize}) null")}
                    ----
                    {MySqlMagic.CreateColumnIfNotExistsSql(schema, tableName, "leased_at", "datetime(6) null")}
                    ----
                    {MySqlMagic.DropIndexIfExistsSql(schema, tableName, "idx_receive")}
                    ----
                    {MySqlMagic.CreateIndexIfNotExistsSql(schema, tableName, "idx_receive_lease", "visible, expiration, processing, leased_until")}");
            }
        }

        /// <summary>
        /// Handle retrieving a message from the queue, decoding it, and performing any transaction maintenance.
        /// </summary>
        /// <param name="context">Transaction context the receive is operating on</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        protected override async Task<TransportMessage> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken)
        {
            TransportMessage transportMessage;

            using (var connection = await _connectionProvider.GetConnectionAsync())
            {
                using (var command = connection.CreateCommand())
                {
                    var tableName = _receiveTableName.QualifiedName;
                    command.CommandText = $@"
                        SELECT id INTO @id
                        FROM {tableName} 
                        WHERE visible < now(6) AND 
                              expiration > now(6) AND 
                              processing = 0 AND 
                              1 = CASE
					            WHEN leased_until is null then 1
					            WHEN date_add(date_add(leased_until, INTERVAL @lease_tolerance_total_seconds SECOND), INTERVAL @lease_tolerance_microseconds MICROSECOND) < now(6) THEN 1
					            ELSE 0
				              END 
                        ORDER BY priority DESC, 
                                 visible ASC, 
                                 id ASC 
                        LIMIT 1
                        FOR UPDATE;

                        SELECT id,
                               headers,
                               body
                        FROM {tableName}
                        WHERE id = @id
                        LIMIT 1;

                        UPDATE {tableName} 
                        SET processing = 1,
                            leased_until = date_add(date_add(now(6), INTERVAL @lease_total_seconds SECOND), INTERVAL @lease_microseconds MICROSECOND),
                            leased_at = now(6),
                            leased_by = @leased_by
                        WHERE id = @id;

                        SET @id = null;";
                    command.Parameters.Add("lease_total_seconds", MySqlDbType.Int32).Value = (int)_leaseInterval.TotalSeconds;
                    command.Parameters.Add("lease_microseconds", MySqlDbType.Int32).Value = _leaseInterval.Milliseconds * 1000;
                    command.Parameters.Add("lease_tolerance_total_seconds", MySqlDbType.Int32).Value = (int)_leaseTolerance.TotalSeconds;
                    command.Parameters.Add("lease_tolerance_microseconds", MySqlDbType.Int32).Value = _leaseTolerance.Milliseconds * 1000;
                    command.Parameters.Add("leased_by", MySqlDbType.VarChar, LeasedByColumnSize).Value = _leasedByFactory();
                    try
                    {
                        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                        {
                            transportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken).ConfigureAwait(false);
                            if (transportMessage == null) return null;

                            var messageId = (long)reader["id"];
                            ApplyTransactionSemantics(context, messageId, cancellationToken);
                        }
                    }
                    catch (MySqlException exception) when (exception.Number == (int)MySqlErrorCode.LockDeadlock)
                    {
                        // If we get a transaction deadlock here, simply return null and assume there is nothing to process
                        return null;
                    }
                    catch (Exception exception) when (cancellationToken.IsCancellationRequested)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", exception);
                    }
                }

                await connection.CompleteAsync();
            }

            return transportMessage;
        }

        /// <summary>
        /// Responsible for releasing the lease on message failure and removing the message on transaction commit
        /// </summary>
        /// <param name="context">Transaction context of the message processing</param>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        private void ApplyTransactionSemantics(ITransactionContext context, long messageId, CancellationToken cancellationToken)
        {
            AutomaticLeaseRenewer renewal = null;
            if (_automaticLeaseRenewal)
            {
                renewal = new AutomaticLeaseRenewer(this, _receiveTableName.QualifiedName, messageId, _connectionProvider, _automaticLeaseRenewalInterval, _leaseInterval, cancellationToken);
            }

            context.OnAborted(
                ctx =>
                {
                    renewal?.Dispose();
                    try
                    {
                        AsyncHelpers.RunSync(() => UpdateLease(_connectionProvider, _receiveTableName.QualifiedName, messageId, null, cancellationToken));
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "While Resetting Lease");
                    }
                }
            );

            context.OnCommitted(
                async ctx =>
                {
                    renewal?.Dispose();
                    try
                    {
                        await DeleteMessage(messageId);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "While Deleting Message");
                    }
                }
            );
        }

        /// <summary>
        /// Gets the outbound message buffer for sending of messages
        /// </summary>
        /// <param name="context">Transaction context containing the message buffer</param>
        ConcurrentQueue<AddressedTransportMessage> GetOutboundMessageBuffer(ITransactionContext context)
        {
            return context.GetOrAdd(OutboundMessageBufferKey, () =>
                {
                    var outgoingMessages = new ConcurrentQueue<AddressedTransportMessage>();

                    async Task SendOutgoingMessages(ITransactionContext _)
                    {
                        using (var connection = await _connectionProvider.GetConnectionAsync())
                        {
                            while (outgoingMessages.IsEmpty == false)
                            {
                                if (outgoingMessages.TryDequeue(out var addressed) == false)
                                {
                                    break;
                                }

                                await InnerSend(addressed.DestinationAddress, addressed.Message, connection);
                            }

                            await connection.CompleteAsync();
                        }
                    }

                    context.OnCommitted(SendOutgoingMessages);

                    return outgoingMessages;
                }
            );
        }

        /// <summary>
        /// Updates a lease with a new leased_until value
        /// </summary>
        /// <param name="connectionProvider">Provider for obtaining a connection</param>
        /// <param name="tableName">Name of the table the messages are stored in</param>
        /// <param name="messageId">Identifier of the message whose lease is being updated</param>
        /// <param name="leaseInterval">New lease interval. If <c>null</c> the lease will be released</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        protected virtual async Task UpdateLease(IDbConnectionProvider connectionProvider, string tableName, long messageId, TimeSpan? leaseInterval, CancellationToken cancellationToken)
        {
            using (var connection = await connectionProvider.GetConnectionAsync())
            {
                using (var command = connection.CreateCommand())
                {
                    if (leaseInterval.HasValue)
                    {
                        command.CommandText = $@"
                            UPDATE {tableName}
                            SET leased_until = date_add(date_add(now(6), INTERVAL @lease_total_seconds SECOND), INTERVAL @lease_microseconds MICROSECOND)
                            WHERE id = @id";
                        command.Parameters.Add("id", MySqlDbType.Int64).Value = messageId;
                        command.Parameters.Add("lease_total_seconds", MySqlDbType.Int32).Value = (int)leaseInterval.Value.TotalSeconds;
                        command.Parameters.Add("lease_microseconds", MySqlDbType.Int32).Value = leaseInterval.Value.Milliseconds * 1000;
                    }
                    else
                    {
                        command.CommandText = $@"
                            UPDATE {tableName}
                            SET processing = 0,
                                leased_until = null,
                                leased_by = null,
                                leased_at = null
                            WHERE id = @id";
                        command.Parameters.Add("id", MySqlDbType.Int64).Value = messageId;
                    }
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                await connection.CompleteAsync();
            }
        }

        /// <summary>
        /// Handles automatically renewing a lease for a given message
        /// </summary>
        class AutomaticLeaseRenewer : IDisposable
        {
            private readonly MySqlLeaseTransport _leaseTransport;
            readonly string _tableName;
            readonly long _messageId;
            readonly IDbConnectionProvider _connectionProvider;
            readonly TimeSpan _leaseInterval;
            readonly CancellationToken _cancellationToken;
            Timer _renewTimer;

            public AutomaticLeaseRenewer(MySqlLeaseTransport leaseTransport, string tableName, long messageId, IDbConnectionProvider connectionProvider, TimeSpan renewInterval, TimeSpan leaseInterval, CancellationToken cancellationToken)
            {
                _leaseTransport = leaseTransport;
                _tableName = tableName;
                _messageId = messageId;
                _connectionProvider = connectionProvider;
                _leaseInterval = leaseInterval;
                _cancellationToken = cancellationToken;
                _renewTimer = new Timer(RenewLease, null, renewInterval, renewInterval);
            }


            public void Dispose()
            {
                _renewTimer?.Change(TimeSpan.FromMilliseconds(-1), TimeSpan.FromMilliseconds(-1));
                _renewTimer?.Dispose();
                _renewTimer = null;
            }

            async void RenewLease(object state)
            {
                try
                {
                    await _leaseTransport.UpdateLease(_connectionProvider, _tableName, _messageId, _leaseInterval, _cancellationToken);
                }
                catch (Exception ex)
                {
                    _leaseTransport._log.Error(ex, "While Renewing Lease");
                }
            }
        }

        class AddressedTransportMessage
        {
            public string DestinationAddress { get; set; }
            public TransportMessage Message { get; set; }
        }
    }
}
