using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace Rebus.MySql
{
    /// <summary>
    /// Wraps some nice extension methods for <see cref="MySqlConnection"/> that makes it easy e.g. to query the schema
    /// </summary>
    static class MySqlMagic
    {
        /// <summary>
        /// Gets the names of all tables in the current database. MySQL does not use prefixes, but we maintain the
        /// TableName class to make it easier to port the SQL Server code and bring in patches.
        /// </summary>
        public static List<TableName> GetTableNames(this MySqlConnection connection, MySqlTransaction transaction = null)
        {
            var results = new List<TableName>();

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                // Get all tables within our database schema
                command.CommandText = $"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '{connection.Database}'";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = (string)reader["TABLE_NAME"];
                        results.Add(new TableName(string.Empty, tableName));
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Gets the information about columns for a specific table in the database
        /// </summary>
        public static Dictionary<string, string> GetColumns(this MySqlConnection connection, string schema, string tableName, MySqlTransaction transaction = null)
        {
            var results = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                // Use the current database prefix if one is not provided
                if (string.IsNullOrWhiteSpace(schema))
                {
                    schema = connection.Database;
                }

                command.CommandText = $@"
                    SELECT COLUMN_NAME, 
                           DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{schema}' AND 
                          TABLE_NAME = '{tableName}'";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = (string)reader["COLUMN_NAME"];
                        var type = (string)reader["DATA_TYPE"];
                        results[name] = type;
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Creates the SQL to conditionally add a column to a database schema in MySQL
        /// </summary>
        /// <param name="schema">Database schema name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="columnName">Column name</param>
        /// <param name="columnType">Column type</param>
        /// <returns>String representing the column creation SQL</returns>
        public static string CreateColumnIfNotExistsSql(string schema, string tableName, string columnName, string columnType)
        {
            return $@"
                SELECT count(*) INTO @count
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND
                      TABLE_NAME = '{tableName}' AND
                      COLUMN_NAME = '{columnName}'
                LIMIT 1;

                SET @query = IF(@count <= 0, 
                    'ALTER TABLE `{schema}`.`{tableName}` ADD COLUMN `{columnName}` {columnType}',
                    'select \'column exists\' as status');

                PREPARE statement from @query;
                EXECUTE statement;
                SET @count = null;
                SET @query = null;";
        }

        /// <summary>
        /// Creates the SQL to conditionally drop a column from a database schema in MySQL
        /// </summary>
        /// <param name="schema">Database schema name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="columnName">Column name</param>
        /// <returns>String representing the column dropping SQL</returns>
        public static string DropColumnIfExistsSql(string schema, string tableName, string columnName)
        {
            return $@"
                SELECT count(*) INTO @count
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND
                      TABLE_NAME = '{tableName}' AND
                      COLUMN_NAME = '{columnName}'
                LIMIT 1;

                SET @query = IF(@count > 0, 
                    'ALTER TABLE `{schema}`.`{tableName}` DROP COLUMN `{columnName}`',
                    'select \'column does not exist\' as status');

                prepare statement from @query;
                EXECUTE statement;
                SET @count = null;
                SET @query = null;";
        }

        /// <summary>
        /// Gets the information about indexes for a specific table in the database
        /// </summary>
        public static Dictionary<string, string> GetIndexes(this MySqlConnection connection, string schema, string tableName, MySqlTransaction transaction = null)
        {
            var results = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                // Use the current database prefix if one is not provided
                if (string.IsNullOrWhiteSpace(schema))
                {
                    schema = connection.Database;
                }

                command.CommandText = $@"
                    SELECT INDEX_NAME,
                           GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') as COLUMNS
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = '{schema}' AND 
                          TABLE_NAME = '{tableName}'
                    GROUP BY INDEX_NAME";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = (string)reader["INDEX_NAME"];
                        var type = (string)reader["COLUMNS"];
                        results[name] = type;
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Creates the SQL to conditionally add an index to a database schema in MySQL
        /// </summary>
        /// <param name="schema">Database schema name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="indexName">Index name</param>
        /// <param name="indexColumns">Index column list</param>
        /// <returns>String representing the index creation SQL</returns>
        public static string CreateIndexIfNotExistsSql(string schema, string tableName, string indexName, string indexColumns)
        {
            return $@"
                SELECT count(*) INTO @count
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = '{schema}' AND
                      TABLE_NAME = '{tableName}' AND
                      INDEX_NAME = '{indexName}'
                LIMIT 1;

                SET @query = IF(@count <= 0, 
                    'ALTER TABLE `{schema}`.`{tableName}` ADD INDEX `{indexName}` ({indexColumns})',
                    'select \'index exists\' as status');

                prepare statement from @query;
                EXECUTE statement;
                SET @query = null;
                SET @count = null;";
        }

        /// <summary>
        /// Creates the SQL to conditionally drop an index from a database schema in MySQL
        /// </summary>
        /// <param name="schema">Database schema name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="indexName">Index name</param>
        /// <returns>String representing the index dropping  SQL</returns>
        public static string DropIndexIfExistsSql(string schema, string tableName, string indexName)
        {
            return $@"
                SELECT count(*) INTO @count
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE table_schema = '{schema}' AND
                      table_name = '{tableName}' AND
                      index_name = '{indexName}'
                LIMIT 1;

                SET @query = IF(@count > 0, 
                    'ALTER TABLE `{schema}`.`{tableName}` DROP INDEX `{indexName}`',
                    'select \'index does not exist\' as status');

                prepare statement from @query;
                EXECUTE statement;
                SET @count = null;
                SET @query = null;";
        }
    }
}
