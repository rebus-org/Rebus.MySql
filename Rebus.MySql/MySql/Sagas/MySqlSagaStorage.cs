using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.MySql.Sagas.Serialization;
using Rebus.Sagas;

namespace Rebus.MySql.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaStorage"/> that persists saga data as a Newtonsoft JSON.NET-serialized object to a table in MySQL.
    /// Correlation properties are stored in a separate index table, allowing for looking up saga data instanes based on the configured correlation
    /// properties
    /// </summary>
    public class MySqlSagaStorage : ISagaStorage, IInitializable
    {
        const int MaximumSagaDataTypeNameLength = 40;
        const string IdPropertyName = nameof(ISagaData.Id);
        const bool IndexNullProperties = false;

        static readonly Encoding JsonTextEncoding = Encoding.UTF8;

        readonly ILog _log;
        readonly IDbConnectionProvider _connectionProvider;
        readonly ISagaTypeNamingStrategy _sagaTypeNamingStrategy;
        readonly ISagaSerializer _sagaSerializer;
        readonly TableName _dataTableName;
        readonly TableName _indexTableName;

        /// <summary>
        /// Constructs the saga storage, using the specified connection provider and tables for persistence.
        /// </summary>
		public MySqlSagaStorage(IDbConnectionProvider connectionProvider, string dataTableName, string indexTableName, IRebusLoggerFactory rebusLoggerFactory, ISagaTypeNamingStrategy sagaTypeNamingStrategy, ISagaSerializer sagaSerializer)
        {
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _sagaTypeNamingStrategy = sagaTypeNamingStrategy ?? throw new ArgumentNullException(nameof(sagaTypeNamingStrategy));

            _log = rebusLoggerFactory.GetLogger<MySqlSagaStorage>();
            _sagaSerializer = sagaSerializer;

            _dataTableName = TableName.Parse(dataTableName);
            _indexTableName = TableName.Parse(indexTableName);
        }

        /// <summary>
        /// Initializes the storage
        /// </summary>
        public void Initialize()
        {
        }

        /// <summary>
        /// Checks to see if the configured tables exist, creating them if necessary
        /// </summary>
        public void EnsureTablesAreCreated()
        {
            AsyncHelpers.RunSync(EnsureTablesAreCreatedAsync);
        }

        async Task EnsureTablesAreCreatedAsync()
        {
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                var tableNames = connection.GetTableNames().ToList();
                var hasDataTable = tableNames.Contains(_dataTableName);
                var hasIndexTable = tableNames.Contains(_indexTableName);
                if (hasDataTable && hasIndexTable)
                {
                    return;
                }
                if (hasDataTable)
                {
                    throw new RebusApplicationException(
                        $"The saga index table '{_indexTableName.QualifiedName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_dataTableName.QualifiedName}', which was supposed to be created as the data table");
                }
                if (hasIndexTable)
                {
                    throw new RebusApplicationException(
                        $"The saga data table '{_dataTableName.QualifiedName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_indexTableName.QualifiedName}', which was supposed to be created as the index table");
                }

                _log.Info("Saga tables {tableName} (data) and {tableName} (index) do not exist - they will be created now", _dataTableName.QualifiedName, _indexTableName.QualifiedName);

                await connection.ExecuteCommands($@"
                    CREATE TABLE {_dataTableName.QualifiedName} (
                        `id` CHAR(36) NOT NULL,
                        `revision` INT NOT NULL,
                        `data` LONGBLOB NOT NULL,
                        PRIMARY KEY (`id`)
                    );
                    ----
                    CREATE TABLE {_indexTableName.QualifiedName} (
                        `saga_type` VARCHAR(40) NOT NULL,
                        `key` VARCHAR(200) NOT NULL,
                        `value` VARCHAR(200) NOT NULL,
                        `saga_id` CHAR(36) NOT NULL,
                        PRIMARY KEY (`saga_type`, `key`, `value`)
                    );
                    ----
                    CREATE INDEX `idx_saga_id` ON {_indexTableName.QualifiedName} (
                        `saga_id`
                    );").ConfigureAwait(false);
                await connection.Complete().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Queries the saga index for an instance with the given <paramref name="sagaDataType"/> with a
        /// a property named <paramref name="propertyName"/> and the value <paramref name="propertyValue"/>
        /// </summary>
        public async Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            if (sagaDataType == null) throw new ArgumentNullException(nameof(sagaDataType));
            if (propertyName == null) throw new ArgumentNullException(nameof(propertyName));
            if (propertyValue == null) throw new ArgumentNullException(nameof(propertyValue));

            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                using (var command = connection.CreateCommand())
                {
                    if (propertyName.Equals(IdPropertyName, StringComparison.OrdinalIgnoreCase))
                    {
                        command.CommandText = $@"
                            SELECT data 
                            FROM {_dataTableName.QualifiedName} 
                            WHERE id = @value 
                            LIMIT 1";
                    }
                    else
                    {
                        command.CommandText = $@"
                            SELECT s.data AS data 
                            FROM {_dataTableName.QualifiedName} s 
                                JOIN {_indexTableName.QualifiedName} i ON (s.id = i.saga_id) 
                            WHERE i.`saga_type` = @saga_type AND 
                                  i.`key` = @key AND 
                                  i.`value` = @value 
                            LIMIT 1";
                        var sagaTypeName = GetSagaTypeName(sagaDataType);
                        command.Parameters.Add("key", MySqlDbType.VarChar, propertyName.Length).Value = propertyName;
                        command.Parameters.Add("saga_type", MySqlDbType.VarChar, sagaTypeName.Length).Value = sagaTypeName;
                    }
                    var correlationPropertyValue = GetCorrelationPropertyValue(propertyValue);
                    command.Parameters.Add("value", MySqlDbType.VarChar, MathUtil.GetNextPowerOfTwo(correlationPropertyValue.Length)).Value = correlationPropertyValue;
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (!await reader.ReadAsync().ConfigureAwait(false)) return null;
                        var value = GetData(reader);
                        try
                        {
                            var sagaData = _sagaSerializer.DeserializeFromString(sagaDataType, value);
                            return sagaData;
                        }
                        catch (Exception exception)
                        {
                            throw new RebusApplicationException(exception, $"An error occurred while attempting to deserialize '{value}' into a {sagaDataType}");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Serializes the given <see cref="ISagaData"/> and generates entries in the index for the specified <paramref name="correlationProperties"/>
        /// </summary>
        public async Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            if (sagaData.Id == Guid.Empty)
            {
                throw new InvalidOperationException($"Saga data {sagaData.GetType()} has an uninitialized Id property!");
            }
            if (sagaData.Revision != 0)
            {
                throw new InvalidOperationException($"Attempted to insert saga data with ID {sagaData.Id} and revision {sagaData.Revision}, but revision must be 0 on first insert!");
            }
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        INSERT INTO {_dataTableName.QualifiedName} (
                            id, 
                            revision, 
                            data
                        ) VALUES (
                            @id, 
                            @revision, 
                            @data
                        )";
                    var data = _sagaSerializer.SerializeToString(sagaData);
                    command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                    command.Parameters.Add("revision", MySqlDbType.Int32).Value = sagaData.Revision;
                    SetData(command, data);
                    try
                    {
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    catch (MySqlException sqlException)
                    {
                        if (sqlException.Number == (int)MySqlErrorCode.LockDeadlock)
                        {
                            throw new ConcurrencyException($"An exception occurred while attempting to insert saga data with ID {sagaData.Id}");
                        }
                        throw;
                    }
                }

                var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);
                if (propertiesToIndex.Any())
                {
                    await CreateIndex(connection, sagaData, propertiesToIndex).ConfigureAwait(false);
                }
                await connection.Complete().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Updates the given <see cref="ISagaData"/> and generates entries in the index for the specified <paramref name="correlationProperties"/>
        /// </summary>
        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                var revisionToUpdate = sagaData.Revision;
                sagaData.Revision++;
                try
                {
                    // First, delete existing index
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $@"DELETE FROM {_indexTableName.QualifiedName} WHERE saga_id = @id";
                        command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    // Next, update or insert the saga
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $@"
                            UPDATE {_dataTableName.QualifiedName} 
                            SET data = @data,
                                revision = @next_revision 
                            WHERE id = @id AND 
                                  revision = @current_revision";
                        var data = _sagaSerializer.SerializeToString(sagaData);
                        command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                        command.Parameters.Add("current_revision", MySqlDbType.Int32).Value = revisionToUpdate;
                        command.Parameters.Add("next_revision", MySqlDbType.Int32).Value = sagaData.Revision;
                        SetData(command, data);
                        var rows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        if (rows == 0)
                        {
                            throw new ConcurrencyException($"Update of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                        }
                    }
                    var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);
                    if (propertiesToIndex.Any())
                    {
                        await CreateIndex(connection, sagaData, propertiesToIndex).ConfigureAwait(false);
                    }
                    await connection.Complete().ConfigureAwait(false);
                }
                catch
                {
                    sagaData.Revision--;
                    throw;
                }
            }
        }

        /// <summary>
        /// Deletes the given <see cref="ISagaData"/> and removes all its entries in the index
        /// </summary>
        public async Task Delete(ISagaData sagaData)
        {
            using (var connection = await _connectionProvider.GetConnection().ConfigureAwait(false))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"DELETE FROM {_dataTableName.QualifiedName} WHERE id = @id AND revision = @current_revision";
                    command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                    command.Parameters.Add("current_revision", MySqlDbType.Int32).Value = sagaData.Revision;
                    var rows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    if (rows == 0)
                    {
                        throw new ConcurrencyException($"Delete of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                    }
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"DELETE FROM {_indexTableName.QualifiedName} WHERE saga_id = @id";
                    command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                await connection.Complete().ConfigureAwait(false);
            }
            sagaData.Revision++;
        }

        static void SetData(MySqlCommand command, string data)
        {
            var bytes = JsonTextEncoding.GetBytes(data);
            command.Parameters.Add("data", MySqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(bytes.Length)).Value = bytes;
        }

        static string GetData(DbDataReader reader)
        {
            var bytes = (byte[])reader["data"];
            var value = JsonTextEncoding.GetString(bytes);
            return value;
        }

        static string GetCorrelationPropertyValue(object propertyValue)
        {
            return (propertyValue ?? "").ToString();
        }

        async Task CreateIndex(IDbConnection connection, ISagaData sagaData, IEnumerable<KeyValuePair<string, string>> propertiesToIndex)
        {
            var sagaTypeName = GetSagaTypeName(sagaData.GetType());
            var propertiesToIndexList = propertiesToIndex.ToList();

            var parameters = propertiesToIndexList
                .Select((p, i) => new
                {
                    PropertyName = p.Key,
                    PropertyValue = GetCorrelationPropertyValue(p.Value),
                    PropertyNameParameter = $"n{i}",
                    PropertyValueParameter = $"v{i}"
                })
                .ToList();

            // lastly, generate new index
            using (var command = connection.CreateCommand())
            {
                // Generate batch insert with SQL for each entry in the index
                var inserts = parameters
                    .Select(a => $@"
                        INSERT INTO {_indexTableName.QualifiedName}
                            (`saga_type`, `key`, `value`, `saga_id`) 
                        VALUES
                            (@saga_type, @{a.PropertyNameParameter}, @{a.PropertyValueParameter}, @saga_id)")
                    .ToList();
                var sql = string.Join(";" + Environment.NewLine, inserts);
                command.CommandText = sql;
                foreach (var parameter in parameters)
                {
                    var propertyName = parameter.PropertyName;
                    var propertyValue = parameter.PropertyValue;
                    command.Parameters.Add(parameter.PropertyNameParameter, MySqlDbType.VarChar, propertyName.Length).Value = propertyName;
                    command.Parameters.Add(parameter.PropertyValueParameter, MySqlDbType.VarChar, MathUtil.GetNextPowerOfTwo(propertyValue.Length)).Value = propertyValue;
                }
                command.Parameters.Add("saga_type", MySqlDbType.VarChar, sagaTypeName.Length).Value = sagaTypeName;
                command.Parameters.Add("saga_id", MySqlDbType.Guid).Value = sagaData.Id;
                try
                {
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                catch (MySqlException sqlException)
                {
                    if (sqlException.Number == (int)MySqlErrorCode.MultiplePrimaryKey)
                    {
                        throw new ConcurrencyException($"Could not update index for saga with ID {sagaData.Id} because of a PK violation - there must already exist a saga instance that uses one of the following correlation properties: {string.Join(", ", propertiesToIndexList.Select(p => $"{p.Key}='{p.Value}'"))}");
                    }
                    throw;
                }
            }
        }

        string GetSagaTypeName(Type sagaDataType)
        {
            var sagaTypeName = _sagaTypeNamingStrategy.GetSagaTypeName(sagaDataType, MaximumSagaDataTypeNameLength);
            if (sagaTypeName == null)
            {
                throw new Exception($"{_sagaTypeNamingStrategy.GetType().FullName} generated a NULL saga type name for {sagaDataType.FullName}.");
            }
            if (sagaTypeName.Length > MaximumSagaDataTypeNameLength)
            {
                throw new InvalidOperationException(
                    $@"Sorry, but the maximum length of the name of a saga data class is currently limited to {MaximumSagaDataTypeNameLength} characters!
This is due to a limitation in MySQL, where compound indexes have a 900 byte upper size limit - and
since the saga index needs to be able to efficiently query by saga type, key, and value at the same time,
there's room for only 200 characters as the key, 200 characters as the value, and 40 characters as the
saga type name.");
            }

            return sagaTypeName;
        }

        static List<KeyValuePair<string, string>> GetPropertiesToIndex(ISagaData sagaData,
            IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            return correlationProperties
                .Select(p => p.PropertyName)
                .Select(path =>
                {
                    var value = Reflect.Value(sagaData, path);
                    return new KeyValuePair<string, string>(path, value?.ToString());
                })
                .Where(kvp => IndexNullProperties || kvp.Value != null)
                .ToList();
        }
    }
}
