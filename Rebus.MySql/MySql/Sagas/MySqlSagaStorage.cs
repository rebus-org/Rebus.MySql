using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.MySql.Extensions;
using Rebus.MySql.Reflection;

namespace Rebus.MySql.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaStorage"/> that uses MySQL to do its thing
    /// </summary>
    public class MySqlSagaStorage : ISagaStorage
    {
        static readonly string IdPropertyName = Reflect.Path<ISagaData>(d => d.Id);

        readonly ObjectSerializer _objectSerializer = new ObjectSerializer();
        readonly MySqlConnectionHelper _connectionHelper;
        readonly string _dataTableName;
        readonly string _indexTableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the saga storage
        /// </summary>
        public MySqlSagaStorage(MySqlConnectionHelper connectionHelper, string dataTableName, string indexTableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionHelper == null) throw new ArgumentNullException(nameof(connectionHelper));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper;
            _dataTableName = dataTableName;
            _indexTableName = indexTableName;
            _log = rebusLoggerFactory.GetLogger<MySqlSagaStorage>();
        }

        /// <summary>
        /// Finds an already-existing instance of the given saga data type that has a property with the given <paramref name="propertyName" />
        /// whose value matches <paramref name="propertyValue" />. Returns null if no such instance could be found
        /// </summary>
        public async Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    if (propertyName == IdPropertyName)
                    {
                        command.CommandText = $@"
                            SELECT s.`data`
                                FROM `{_dataTableName}` s
                                WHERE s.`id` = @id
                            ";
                        command.Parameters.Add(command.CreateParameter("id", DbType.Guid, ToGuid(propertyValue)));
                    }
                    else
                    {
                        command.CommandText =
                            $@"
                                SELECT s.`data`
                                    FROM `{_dataTableName}` s
                                    JOIN `{_indexTableName}` i on s.id = i.saga_id
                                    WHERE i.`saga_type` = @saga_type AND i.`key` = @key AND i.value = @value;
                                ";

                        command.Parameters.Add(command.CreateParameter("key", DbType.String, propertyName));
                        command.Parameters.Add(command.CreateParameter("saga_type", DbType.String, GetSagaTypeName(sagaDataType)));
                        command.Parameters.Add(command.CreateParameter("value", DbType.String, (propertyValue ?? "").ToString()));
                    }

                    var data = (byte[])await command.ExecuteScalarAsync();

                    if (data == null) return null;

                    try
                    {
                        var sagaData = (ISagaData)_objectSerializer.Deserialize(data);

                        if (!sagaDataType.GetTypeInfo().IsInstanceOfType(sagaData))
                        {
                            return null;
                        }

                        return sagaData;
                    }
                    catch (Exception exception)
                    {
                        var message =
                            $"An error occurred while attempting to deserialize '{data}' into a {sagaDataType}";

                        throw new RebusApplicationException(exception, message);
                    }
                    finally
                    {
                        connection.Complete();
                    }
                }
            }
        }

        /// <summary>
        /// Inserts the given saga data as a new instance. Throws a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if another saga data instance
        /// already exists with a correlation property that shares a value with this saga data.
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

            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));
                    command.Parameters.Add(command.CreateParameter("revision", DbType.Int32, sagaData.Revision));
                    command.Parameters.Add(command.CreateParameter("data", DbType.Binary, _objectSerializer.Serialize(sagaData)));

                    command.CommandText =
                        $@"
                            INSERT
                                INTO `{_dataTableName}` (`id`, `revision`, `data`)
                                VALUES (@id, @revision, @data);

                            ";

                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (MySqlException exception)
                    {
                        throw new ConcurrencyException(exception, $"Saga data {sagaData.GetType()} with ID {sagaData.Id} in table {_dataTableName} could not be inserted!");
                    }
                }

                var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);

                if (propertiesToIndex.Any())
                {
                    await CreateIndex(sagaData, connection, propertiesToIndex);
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Updates the already-existing instance of the given saga data, throwing a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if another
        /// saga data instance exists with a correlation property that shares a value with this saga data, or if the saga data
        /// instance no longer exists.
        /// </summary>
        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var revisionToUpdate = sagaData.Revision;

                sagaData.Revision++;

                var nextRevision = sagaData.Revision;

                // first, delete existing index
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"

                        DELETE FROM `{_indexTableName}` WHERE `saga_id` = @id;

                        ";
                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));
                    await command.ExecuteNonQueryAsync();
                }

                // next, update or insert the saga
                using (var command = connection.CreateCommand())
                {
                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));
                    command.Parameters.Add(command.CreateParameter("current_revision", DbType.Int32, revisionToUpdate));
                    command.Parameters.Add(command.CreateParameter("next_revision", DbType.Int32, nextRevision));
                    command.Parameters.Add(command.CreateParameter("data", DbType.Binary, _objectSerializer.Serialize(sagaData)));

                    command.CommandText =
                        $@"
                            UPDATE `{_dataTableName}`
                                SET `data` = @data, `revision` = @next_revision
                                WHERE `id` = @id AND `revision` = @current_revision;
                            ";

                    var rows = await command.ExecuteNonQueryAsync();

                    if (rows == 0)
                    {
                        throw new ConcurrencyException($"Update of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                    }
                }

                var propertiesToIndex = GetPropertiesToIndex(sagaData, correlationProperties);

                if (propertiesToIndex.Any())
                {
                    await CreateIndex(sagaData, connection, propertiesToIndex);
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Deletes the saga data instance, throwing a <see cref="T:Rebus.Exceptions.ConcurrencyException" /> if the instance no longer exists
        /// </summary>
        public async Task Delete(ISagaData sagaData)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            DELETE
                                FROM `{_dataTableName}`
                                WHERE `id` = @id AND `revision` = @current_revision;

                            ";

                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));
                    command.Parameters.Add(command.CreateParameter("current_revision", DbType.Int32, sagaData.Revision));

                    var rows = await command.ExecuteNonQueryAsync();

                    if (rows == 0)
                    {
                        throw new ConcurrencyException($"Delete of saga with ID {sagaData.Id} did not succeed because someone else beat us to it");
                    }
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            DELETE
                                FROM `{_indexTableName}`
                                WHERE `saga_id` = @id

                            ";
                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));

                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }

            sagaData.Revision++;
        }

        /// <summary>
        /// Checks to see if the configured saga data and saga index table exists. If they both exist, we'll continue, if
        /// neigther of them exists, we'll try to create them. If one of them exists, we'll throw an error.
        /// </summary>
        public async Task EnsureTablesAreCreated()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames().ToHashSet();

                var hasDataTable = tableNames.Contains(_dataTableName);
                var hasIndexTable = tableNames.Contains(_indexTableName);

                if (hasDataTable && hasIndexTable)
                {
                    return;
                }

                if (hasDataTable)
                {
                    throw new RebusApplicationException(
                        $"The saga index table '{_indexTableName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_dataTableName}', which was supposed to be created as the data table");
                }

                if (hasIndexTable)
                {
                    throw new RebusApplicationException(
                        $"The saga data table '{_dataTableName}' does not exist, so the automatic saga schema generation tried to run - but there was already a table named '{_indexTableName}', which was supposed to be created as the index table");
                }

                _log.Info("Saga tables '{0}' (data) and '{1}' (index) do not exist - they will be created now", _dataTableName, _indexTableName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            CREATE TABLE `{_dataTableName}` (
                                `id` CHAR(36) NOT NULL,
                                `revision` INTEGER NOT NULL,
                                `data` MEDIUMBLOB NOT NULL,
                                PRIMARY KEY (`id`)
                            );";

                    command.ExecuteNonQuery();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        CREATE TABLE `{_indexTableName}` (
                            `saga_type` TEXT NOT NULL,
                            `key` TEXT NOT NULL,
                            `value` TEXT NOT NULL,
                            `saga_id` CHAR(36) NOT NULL,
                            PRIMARY KEY (`key`(128), `value`(128), `saga_type`(128))
                        );

                        CREATE INDEX `idx_{_indexTableName}` ON `{_indexTableName}` (`saga_id`);
                        ";

                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }

        private static object ToGuid(object propertyValue)
        {
            return Convert.ChangeType(propertyValue, typeof(Guid));
        }

        string GetSagaTypeName(Type sagaDataType)
        {
            return sagaDataType.FullName;
        }

        static List<KeyValuePair<string, string>> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            return correlationProperties
                .Select(p => p.PropertyName)
                .Select(path =>
                {
                    var value = Reflect.Value(sagaData, path);

                    return new KeyValuePair<string, string>(path, value?.ToString());
                })
                .Where(kvp => kvp.Value != null)
                .ToList();
        }

        async Task CreateIndex(ISagaData sagaData, MySqlConnection connection, IEnumerable<KeyValuePair<string, string>> propertiesToIndex)
        {
            var sagaTypeName = GetSagaTypeName(sagaData.GetType());
            var parameters = propertiesToIndex
                .Select((p, i) => new
                {
                    PropertyName = p.Key,
                    PropertyValue = p.Value ?? "",
                    PropertyNameParameter = $"@n{i}",
                    PropertyValueParameter = $"@v{i}"
                })
                .ToList();

            // lastly, generate new index
            using (var command = connection.CreateCommand())
            {
                // generate batch insert with SQL for each entry in the index
                var inserts = parameters
                    .Select(a =>
                        $@"
                            INSERT
                                INTO `{_indexTableName}` (`saga_type`, `key`, `value`, `saga_id`)
                                VALUES (@saga_type, {a.PropertyNameParameter}, {a.PropertyValueParameter}, @saga_id)

                            ");

                var sql = string.Join(";" + Environment.NewLine, inserts);

                command.CommandText = sql;

                foreach (var parameter in parameters)
                {
                    command.Parameters.Add(command.CreateParameter(parameter.PropertyNameParameter, DbType.String, parameter.PropertyName));
                    command.Parameters.Add(command.CreateParameter(parameter.PropertyValueParameter, DbType.String, parameter.PropertyValue));
                }

                command.Parameters.Add(command.CreateParameter("saga_type", DbType.String, sagaTypeName));
                command.Parameters.Add(command.CreateParameter("saga_id", DbType.Guid, sagaData.Id));
                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (MySqlException exception)
                {
                    if (exception.Number == MySqlServerMagic.PrimaryKeyViolationNumber)
                    {
                        throw new ConcurrencyException(exception, $"Saga data {sagaData.GetType()} with ID {sagaData.Id} in table {_dataTableName} could not be inserted!");
                    }
                    throw;
                }

            }
        }
    }
}