using MySql.Data.MySqlClient;
using NUnit.Framework;

namespace Rebus.MySql.Tests.Assumptions
{
    [TestFixture, Category(Categories.MySql)]
    public class TestMySqlMagic
    {
        private static void CreateTables(MySqlConnection connection, string tableName1, string tableName2 = null)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $@"
                CREATE TABLE {tableName1} 
                (
                    `id1` BIGINT NOT NULL AUTO_INCREMENT,
                    `col1` MEDIUMBLOB NOT NULL,
                    PRIMARY KEY (`id1`)
                );";
            if (tableName2 != null)
            {
                command.CommandText += $@"
                CREATE TABLE {tableName2}
                (
                    `id2` BIGINT NOT NULL AUTO_INCREMENT,
                    `col2` LONGBLOB NOT NULL,
                    PRIMARY KEY (`id2`)
                );";
            }
            command.ExecuteNonQuery();
        }

        [Test, Description("Simple test to ensure the GetTableNames function works")]
        public void TableNamesWorks()
        {
            // Drop all tables first
            MySqlTestHelper.DropAllTables();

            // Should have no tables in our database schema
            using var connection = new MySqlConnection(MySqlTestHelper.ConnectionString);
            connection.Open();
            var tableNames = connection.GetTableNames();
            Assert.AreEqual(0, tableNames.Count);

            // Now create two tables so we can see it works
            const string tableName1 = "tableName1";
            const string tableName2 = "tableName2";
            CreateTables(connection, tableName1, tableName2);

            // Check they are now there
            tableNames = connection.GetTableNames();
            Assert.AreEqual(2, tableNames.Count);
            var table1 = new TableName("", tableName1);
            Assert.IsTrue(tableNames.Contains(table1));
            var table2 = new TableName("", tableName2);
            Assert.IsTrue(tableNames.Contains(table2));
        }

        [Test, Description("Simple test to ensure the GetColumns function works")]
        public void GetColumnsWorks()
        {
            // Drop all tables first
            MySqlTestHelper.DropAllTables();

            // Now get the columns for system tables so we know a prefix works
            using var connection = new MySqlConnection(MySqlTestHelper.ConnectionString);
            connection.Open();
            var columns = connection.GetColumns("information_schema", "tables");
            Assert.AreEqual("varchar", columns["TABLE_CATALOG"]);
            Assert.AreEqual("bigint", columns["VERSION"]);
            Assert.AreEqual("datetime", columns["update_time"]);

            // Now create two tables so we can see it works for our tables without a schema prefix
            const string tableName1 = "tableName1";
            const string tableName2 = "tableName2";
            CreateTables(connection, tableName1, tableName2);
            columns = connection.GetColumns("", tableName1);
            Assert.AreEqual(2, columns.Count);
            Assert.AreEqual("bigint", columns["id1"]);
            Assert.AreEqual("mediumblob", columns["col1"]);
            columns = connection.GetColumns("", tableName2);
            Assert.AreEqual(2, columns.Count);
            Assert.AreEqual("bigint", columns["id2"]);
            Assert.AreEqual("longblob", columns["col2"]);
        }

        [Test, Description("Test code to create columns conditionally")]
        public void CreateAndDropColumnAndIndexSql()
        {
            // Drop all tables first
            MySqlTestHelper.DropAllTables();

            // Open a connection
            using var connection = new MySqlConnection(MySqlTestHelper.ConnectionString);
            connection.Open();

            // Create a table to test with
            var schema = connection.Database;
            const string tableName = "tableName";
            CreateTables(connection, tableName);

            // This column should already exist, so it should do nothing
            var sql = MySqlMagic.CreateColumnIfNotExistsSql(schema, tableName, "col1", "varchar(255) null");
            var command = connection.CreateCommand();
            command.CommandText = sql;
            var result = command.ExecuteScalar() as string;
            Assert.AreEqual("column exists", result);

            // Now create a new column
            sql = MySqlMagic.CreateColumnIfNotExistsSql(schema, tableName, "date1", "datetime(6) null");
            command = connection.CreateCommand();
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual(null, result);
            var columns = connection.GetColumns("", tableName);
            Assert.AreEqual(3, columns.Count);
            Assert.AreEqual("bigint", columns["id1"]);
            Assert.AreEqual("mediumblob", columns["col1"]);
            Assert.AreEqual("datetime", columns["date1"]);

            // Now lets add an index
            sql = MySqlMagic.CreateIndexIfNotExistsSql(schema, tableName, "idx1", "col1(10), date1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual(null, result);

            // Check the index got added
            var indexes = connection.GetIndexes("", tableName);
            Assert.AreEqual(2, indexes.Count);
            Assert.AreEqual("id1", indexes["PRIMARY"]);
            Assert.AreEqual("col1,date1", indexes["idx1"]);

            // Try again and it should exist
            sql = MySqlMagic.CreateIndexIfNotExistsSql(schema, tableName, "idx1", "col1(10), date1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual("index exists", result);

            // Now drop the index
            sql = MySqlMagic.DropIndexIfExistsSql(schema, tableName, "idx1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual(null, result);

            // Check the index got removed
            indexes = connection.GetIndexes("", tableName);
            Assert.AreEqual(1, indexes.Count);
            Assert.AreEqual("id1", indexes["PRIMARY"]);

            // Try again and it should not exist
            sql = MySqlMagic.DropIndexIfExistsSql(schema, tableName, "idx1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual("index does not exist", result);

            // Now drop the column
            sql = MySqlMagic.DropColumnIfExistsSql(schema, tableName, "date1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual(null, result);
            columns = connection.GetColumns("", tableName);
            Assert.AreEqual(2, columns.Count);
            Assert.AreEqual("bigint", columns["id1"]);
            Assert.AreEqual("mediumblob", columns["col1"]);

            // Now drop the column again and nothing happens
            sql = MySqlMagic.DropColumnIfExistsSql(schema, tableName, "date1");
            command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandText = sql;
            result = command.ExecuteScalar() as string;
            Assert.AreEqual("column does not exist", result);
        }
    }
}
