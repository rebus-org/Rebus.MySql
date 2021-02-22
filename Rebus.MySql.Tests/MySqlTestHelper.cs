using System;
using System.Collections.Generic;
using MySqlConnector;
using Rebus.Exceptions;
using Rebus.MySql.Tests.Extensions;
using Rebus.Tests.Contracts;

namespace Rebus.MySql.Tests
{
    public static class MySqlTestHelper
    {
        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');
        static bool _databaseHasBeenInitialized;
        static string _connectionString;

        public static string ConnectionString
        {
            get
            {
                if (_connectionString != null)
                {
                    return _connectionString;
                }

                var databaseName = DatabaseName;

                if (!_databaseHasBeenInitialized)
                {
                    InitializeDatabase(databaseName);
                }

                Console.WriteLine("Using local SQL database {0}", databaseName);

                _connectionString = GetConnectionStringForDatabase(databaseName);

                return _connectionString;
            }
        }

        public static void Execute(string sql)
        {
            using var connection = new MySqlConnection(ConnectionString);
            connection.Open();

            Console.Write($"SQL => {sql}        -- ");

            using var command = connection.CreateCommand();
            command.CommandText = sql;

            try
            {
                command.ExecuteNonQuery();

                Console.WriteLine("OK");
            }
            catch
            {
                Console.WriteLine("Fail");
                throw;
            }
        }

        public static void DropTable(string tableName)
        {
            DropTable(TableName.Parse(tableName));
        }

        public static void DropTable(TableName tableName)
        {
            DropObject($"DROP TABLE {tableName.QualifiedName}", connection =>
            {
                var tableNames = connection.GetTableNames();
                return tableNames.Contains(tableName);
            });
        }

        static void DropObject(string sqlCommand, Func<MySqlConnection, bool> executeCriteria)
        {
            try
            {
                using var connection = new MySqlConnection(ConnectionString);
                connection.Open();

                var shouldExecute = executeCriteria(connection);
                if (!shouldExecute) return;

                try
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = sqlCommand;
                        command.ExecuteNonQuery();
                    }

                    Console.WriteLine($"SQL OK: {sqlCommand}");
                }
                catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.BadTable)
                {
                    // it's alright
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $@"Could not execute '{sqlCommand}'");
            }
        }

        public static IEnumerable<T> Query<T>(string query)
        {
            using var connection = new MySqlConnection(ConnectionString);
            connection.Open();

            foreach (var result in connection.Query<T>(query))
            {
                yield return result;
            }
        }

        public static void DropAllTables()
        {
            var tableNames = GetTableNames();
            var troublesomeTables = new List<TableName>();

            foreach (var tableName in tableNames)
            {
                try
                {
                    DropTable(tableName);
                }
                catch
                {
                    // most likely because of a FK constraint...
                    troublesomeTables.Add(tableName);
                }
            }

            // try again - this time, at least we know the first level of child tables is gone, which is what's needed now... if multiple levels of FK references are introduced in the future, it might require more passes to do this
            foreach (var tableName in troublesomeTables)
            {
                DropTable(tableName);
            }
        }

        public static List<TableName> GetTableNames()
        {
            using var connection = new MySqlConnection(ConnectionString);
            connection.Open();
            return connection.GetTableNames();
        }

        static void InitializeDatabase(string databaseName)
        {
            try
            {
                var masterConnectionString = GetConnectionStringForDatabase("information_schema");

                using (var connection = new MySqlConnection(masterConnectionString))
                {
                    connection.Open();

                    using var command = connection.CreateCommand();
                    command.CommandText = $"CREATE DATABASE `{databaseName}`";

                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.DatabaseCreateExists)
                    {
                    }

                    Console.WriteLine("Created database {0}", databaseName);
                }

                _databaseHasBeenInitialized = true;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not initialize database '{databaseName}'");
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_MYSQL")
                   ?? $"server=localhost; port=3306; database={databaseName}; user id=mysql; password=mysql;maximum pool size=30;allow user variables=true;";
        }
    }
}
