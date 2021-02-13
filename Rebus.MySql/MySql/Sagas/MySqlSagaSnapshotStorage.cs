using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.MySql.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaSnapshotStorage"/> that uses a table in MySQL to store saga snapshots
    /// </summary>
    public class MySqlSagaSnapshotStorage : ISagaSnapshotStorage
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        static readonly ObjectSerializer DataSerializer = new ObjectSerializer();
        static readonly HeaderSerializer MetadataSerializer = new HeaderSerializer();

        /// <summary>
        /// Constructs the snapshot storage
        /// </summary>
        public MySqlSagaSnapshotStorage(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _log = rebusLoggerFactory.GetLogger<MySqlSagaSnapshotStorage>();
            _tableName = TableName.Parse(tableName);
        }

        /// <summary>
        /// Creates the subscriptions table if necessary
        /// </summary>
        public void EnsureTableIsCreated()
        {
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
            using (var connection = await _connectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName.QualifiedName);

                await connection.ExecuteCommands($@"
                    CREATE TABLE {_tableName.QualifiedName} (
                        `id` CHAR(36) NOT NULL,
                        `revision` INT NOT NULL,
                        `data` LONGTEXT NOT NULL,
                        `metadata` LONGTEXT NOT NULL,
                        PRIMARY KEY (`id`, `revision`)
                    )").ConfigureAwait(false);
                await connection.Complete().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Saves a snapshot of the saga data along with the given metadata
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        INSERT INTO {_tableName.QualifiedName} (
                            `id`,
                            `revision`,
                            `data`,
                            `metadata`
                        ) VALUES (
                            @id, 
                            @revision, 
                            @data,
                            @metadata
                        )";
                    var dataString = DataSerializer.SerializeToString(sagaData);
                    var metadataString = MetadataSerializer.SerializeToString(sagaAuditMetadata);
                    command.Parameters.Add("id", MySqlDbType.Guid).Value = sagaData.Id;
                    command.Parameters.Add("revision", MySqlDbType.Int32).Value = sagaData.Revision;
                    command.Parameters.Add("data", MySqlDbType.VarChar, MathUtil.GetNextPowerOfTwo(dataString.Length)).Value = dataString;
                    command.Parameters.Add("metadata", MySqlDbType.VarChar, MathUtil.GetNextPowerOfTwo(metadataString.Length)).Value = metadataString;
                    Console.WriteLine($"OK WE'RE SAVING SAGA SNAPSHOT {sagaData.Id} rev. {sagaData.Revision} NOW");
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                await connection.Complete().ConfigureAwait(false);
            }
        }
    }
}
