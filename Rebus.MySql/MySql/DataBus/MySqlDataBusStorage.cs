using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Bus;
using Rebus.DataBus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Time;
// ReSharper disable SimplifyLinqExpression

namespace Rebus.MySql.DataBus
{
    /// <summary>
    /// Implementation of <see cref="IDataBusStorage"/> that uses MySQL to store data
    /// </summary>
    public class MySqlDataBusStorage : IDataBusStorage, IDataBusStorageManagement, IInitializable
    {
        static readonly Encoding TextEncoding = Encoding.UTF8;
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly bool _ensureTableIsCreated;
        readonly ILog _log;
        readonly int _commandTimeout;
        readonly IRebusTime _rebusTime;

        /// <summary>
        /// Creates the data storage
        /// </summary>
        public MySqlDataBusStorage(IDbConnectionProvider connectionProvider, string tableName, bool ensureTableIsCreated, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime, int commandTimeout)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _tableName = TableName.Parse(tableName);
            _ensureTableIsCreated = ensureTableIsCreated;
            _commandTimeout = commandTimeout;
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _log = rebusLoggerFactory.GetLogger<MySqlDataBusStorage>();
        }

        /// <summary>
        /// Initializes the MySQL data storage.
        /// Will create the data table, unless this has been explicitly turned off when configuring the data storage
        /// </summary>
        public void Initialize()
        {
            if (!_ensureTableIsCreated) return;

            try
            {
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }
        }

        async Task EnsureTableIsCreatedAsync()
        {
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                var tableNames = connection.GetTableNames();
                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info("Creating data bus table {tableName}", _tableName.QualifiedName);

                await connection.ExecuteCommands($@"
                    CREATE TABLE {_tableName.QualifiedName} (
                        `id` VARCHAR(200) NOT NULL,
                        `meta` LONGBLOB NOT NULL,
                        `data` LONGBLOB NOT NULL,
                        `creation_time` DATETIME(6) NOT NULL,
                        `last_read_time` DATETIME(6) DEFAULT NULL,
                        PRIMARY KEY (`id`)
                    )").ConfigureAwait(false);
                await connection.Complete().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Saves the data from the given source stream under the given ID
        /// </summary>
        public async Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
        {
            var metadataToWrite = new Dictionary<string, string>(metadata ?? new Dictionary<string, string>())
            {
                [MetadataKeys.SaveTime] = _rebusTime.Now.ToString("O")
            };

            try
            {
                using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
                {
                    using (var command = connection.CreateCommand())
                    {
                        var metadataBytes = TextEncoding.GetBytes(_dictionarySerializer.SerializeToString(metadataToWrite));
                        using (var memoryStream = new MemoryStream())
                        {
                            await source.CopyToAsync(memoryStream);
                            var sourceBytes = memoryStream.ToArray();
                            command.CommandTimeout = _commandTimeout;
                            command.CommandText = $@"
                                INSERT INTO {_tableName.QualifiedName} (
                                    `id`, 
                                    `meta`,
                                    `data`, 
                                    `creation_time`,
                                    `last_read_time`) 
                                VALUES (
                                    @id, 
                                    @meta, 
                                    @data, 
                                    @now,
                                    null
                                )";
                            command.Parameters.Add("id", MySqlDbType.VarChar, 200).Value = id;
                            command.Parameters.Add("meta", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(metadataBytes.Length)).Value = metadataBytes;
                            command.Parameters.Add("data", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(sourceBytes.Length)).Value = sourceBytes;
                            command.Parameters.Add("now", MySqlDbType.DateTime).Value = _rebusTime.Now.DateTime;
                            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                    await connection.Complete().ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not save data with ID {id}");
            }
        }

        /// <summary>
        /// Opens the data stored under the given ID for reading
        /// </summary>
        public async Task<Stream> Read(string id)
        {
            try
            {
                // update last read time quickly
                await UpdateLastReadTime(id).ConfigureAwait(false);
                var objectsToDisposeOnException = new ConcurrentStack<IDisposable>();
                var connection = await _connectionProvider.GetConnection().ConfigureAwait(false);
                objectsToDisposeOnException.Push(connection);
                using (var command = connection.CreateCommand())
                {
                    try
                    {
                        command.CommandTimeout = _commandTimeout;
                        command.CommandText = $@"
                            SELECT data
                            FROM {_tableName.QualifiedName} 
                            WHERE id = @id
                            LIMIT 1";
                        command.Parameters.Add("id", MySqlDbType.VarChar, 200).Value = id;
                        var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess).ConfigureAwait(false);
                        objectsToDisposeOnException.Push(reader);
                        if (!await reader.ReadAsync().ConfigureAwait(false))
                        {
                            throw new ArgumentException($"Row with ID {id} not found");
                        }
                        var dataOrdinal = reader.GetOrdinal("data");
                        var stream = reader.GetStream(dataOrdinal);
                        objectsToDisposeOnException.Push(stream);
                        return new StreamWrapper(stream, new IDisposable[]
                        {
                            // defer closing these until the returned stream is closed
                            reader,
                            connection
                        });
                    }
                    catch
                    {
                        // if something of the above fails, we must dispose these things
                        while (objectsToDisposeOnException.TryPop(out var disposable))
                        {
                            disposable.Dispose();
                        }
                        throw;
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load data with ID {id}");
            }
        }

        async Task UpdateLastReadTime(string id)
        {
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                await UpdateLastReadTime(id, connection).ConfigureAwait(false);
                await connection.Complete().ConfigureAwait(false);
            }
        }

        async Task UpdateLastReadTime(string id, IDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"UPDATE {_tableName.QualifiedName} SET last_read_time = @now WHERE id = @id";
                command.Parameters.Add("now", MySqlDbType.DateTime).Value = _rebusTime.Now.DateTime;
                command.Parameters.Add("id", MySqlDbType.VarChar, 200).Value = id;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Loads the metadata stored with the given ID
        /// </summary>
        public async Task<Dictionary<string, string>> ReadMetadata(string id)
        {
            try
            {
                using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $@"
                            SELECT meta,
                                   last_read_time, 
                                   LENGTH(data) AS length
                            FROM {_tableName.QualifiedName} 
                            WHERE id = @id 
                            LIMIT 1";
                        command.Parameters.Add("id", MySqlDbType.VarChar, 200).Value = id;
                        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            if (!await reader.ReadAsync().ConfigureAwait(false))
                            {
                                throw new ArgumentException($"Row with ID {id} not found");
                            }
                            var bytes = (byte[])reader["meta"];
                            var length = (long)reader["length"];
                            var lastReadTimeDbValue = reader["last_read_time"];
                            var jsonText = TextEncoding.GetString(bytes);
                            var metadata = _dictionarySerializer.DeserializeFromString(jsonText);
                            metadata[MetadataKeys.Length] = length.ToString();
                            if (lastReadTimeDbValue != DBNull.Value)
                            {
                                var lastReadTime = (DateTime)lastReadTimeDbValue;
                                metadata[MetadataKeys.ReadTime] = lastReadTime.ToString("O");
                            }
                            return metadata;
                        }
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load metadata for data with ID {id}");
            }
        }

        /// <summary>Deletes the attachment with the given ID</summary>
        public async Task Delete(string id)
        {
            try
            {
                using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"DELETE FROM {_tableName.QualifiedName} WHERE id = @id";
                        command.Parameters.Add("id", MySqlDbType.VarChar, 200).Value = id;
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    await connection.Complete().ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not delete data with ID {id}");
            }
        }

        /// <summary>
        /// Iterates through IDs of attachments that match the given <paramref name="readTime" /> and <paramref name="saveTime" /> criteria.
        /// </summary>
        public IEnumerable<string> Query(TimeRange readTime = null, TimeRange saveTime = null)
        {
            IDbConnection connection = null;

            AsyncHelpers.RunSync(async () =>
            {
                connection = await _connectionProvider.GetConnection().ConfigureAwait(false);
            });

            using (connection)
            {
                using (var command = connection.CreateCommand())
                {
                    var query = new StringBuilder($"SELECT id FROM {_tableName.QualifiedName} WHERE 1=1");
                    var readTimeFrom = readTime?.From;
                    var readTimeTo = readTime?.To;
                    var saveTimeFrom = saveTime?.From;
                    var saveTimeTo = saveTime?.To;
                    if (readTimeFrom != null)
                    {
                        query.Append(" AND last_read_time >= @read_time_from");
                        command.Parameters.Add("read_time_from", MySqlDbType.DateTime).Value = readTimeFrom.Value.DateTime;
                    }
                    if (readTimeTo != null)
                    {
                        query.Append(" AND last_read_time < @read_time_to");
                        command.Parameters.Add("read_time_to", MySqlDbType.DateTime).Value = readTimeTo.Value.DateTime;
                    }
                    if (saveTimeFrom != null)
                    {
                        query.Append(" AND creation_time >= @save_time_from");
                        command.Parameters.Add("save_time_from", MySqlDbType.DateTime).Value = saveTimeFrom.Value.DateTime;
                    }
                    if (saveTimeTo != null)
                    {
                        query.Append(" AND creation_time < @save_time_to");
                        command.Parameters.Add("save_time_to", MySqlDbType.DateTime).Value = saveTimeTo.Value.DateTime;
                    }
                    command.CommandText = query.ToString();
                    using (var reader = command.ExecuteReader())
                    {
                        var ordinal = reader.GetOrdinal("id");
                        while (reader.Read())
                        {
                            yield return (string)reader[ordinal];
                        }
                    }
                }
            }
        }
    }
}
