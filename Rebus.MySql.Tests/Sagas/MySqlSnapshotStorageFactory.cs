using System.Collections.Generic;
using Rebus.Auditing.Sagas;
using Rebus.MySql.Sagas;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MySql.Tests.Sagas
{
    public class MySqlSnapshotStorageFactory : ISagaSnapshotStorageFactory
    {
        const string TableName = "SagaSnaps";

        public MySqlSnapshotStorageFactory()
        {
            MySqlTestHelper.DropTableIfExists(TableName);
        }

        public ISagaSnapshotStorage Create()
        {
            var snapshotStorage = new MySqlSagaSnapshotStorage(MySqlTestHelper.ConnectionHelper, TableName);

            snapshotStorage.EnsureTableIsCreated();

            return snapshotStorage;
        }

        public IEnumerable<SagaDataSnapshot> GetAllSnapshots()
        {
            using (var connection = MySqlTestHelper.ConnectionHelper.GetConnection().Result)
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"SELECT `data`, `metadata` FROM `{TableName}`";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var data = (byte[])reader["data"];
                            var metadataString = (string)reader["metadata"];

                            var objectSerializer = new ObjectSerializer();
                            var dictionarySerializer = new DictionarySerializer();

                            var sagaData = objectSerializer.Deserialize(data);
                            var metadata = dictionarySerializer.DeserializeFromString(metadataString);

                            yield return new SagaDataSnapshot
                            {
                                SagaData = (ISagaData) sagaData,
                                Metadata = metadata
                            };
                        }
                    }
                }
            }
        }
    }
}