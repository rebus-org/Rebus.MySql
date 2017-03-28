using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Extensions;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.MySql.Extensions;

namespace Rebus.MySql.Sagas
{
    /// <summary>
    /// MySql saga snapshot storage that archives a snapshot of the given saga data
    /// </summary>
    public class MySqlSagaSnapshotStorage : ISagaSnapshotStorage
    {
        readonly ObjectSerializer _objectSerializer = new ObjectSerializer();
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly MySqlConnectionHelper _connectionHelper;
        readonly string _tableName;

        /// <summary>
        /// Constructs the storage
        /// </summary>
        public MySqlSagaSnapshotStorage(MySqlConnectionHelper connectionHelper, string tableName)
        {
            if (connectionHelper == null) throw new ArgumentNullException(nameof(connectionHelper));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            _connectionHelper = connectionHelper;
            _tableName = tableName;
        }

        /// <summary>
        /// Archives the given saga data in MySql under its current ID and revision
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            INSERT
                                INTO `{_tableName}` (`id`, `revision`, `data`, `metadata`)
                                VALUES (@id, @revision, @data, @metadata);

                            ";
                    command.Parameters.Add(command.CreateParameter("id", DbType.Guid, sagaData.Id));
                    command.Parameters.Add(command.CreateParameter("revision", DbType.Int32, sagaData.Revision));
                    command.Parameters.Add(command.CreateParameter("data", DbType.Binary, _objectSerializer.Serialize(sagaData)));
                    command.Parameters.Add(command.CreateParameter("metadata", DbType.String, _dictionarySerializer.SerializeToString(sagaAuditMetadata)));

                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Creates the necessary table if it does not already exist
        /// </summary>
        public async Task EnsureTableIsCreated()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames().ToHashSet();

                if (tableNames.Contains(_tableName)) return;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            CREATE TABLE `{_tableName}` (
                                `id` CHAR(36) NOT NULL,
                                `revision` INTEGER NOT NULL,
                                `metadata` MEDIUMTEXT NOT NULL,
                                `data` MEDIUMBLOB NOT NULL,
                                PRIMARY KEY (`id`, `revision`));";

                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }
    }
}