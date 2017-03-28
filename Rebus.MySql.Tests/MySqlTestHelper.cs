using System;
using MySql.Data.MySqlClient;
using Rebus.Tests.Contracts;

namespace Rebus.MySql.Tests
{
    public static class MySqlTestHelper
    {
        const string TableDoesNotExist = "42S02";
        static readonly MySqlConnectionHelper MySqlConnectionHelper = new MySqlConnectionHelper(ConnectionString);
        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');
        public static string ConnectionString => GetConnectionStringForDatabase(DatabaseName);
        public static MySqlConnectionHelper ConnectionHelper => MySqlConnectionHelper;

        public static void DropTableIfExists(string tableName)
        {
            using (var connection = AsyncHelpers.GetSync(() => MySqlConnectionHelper.GetConnection()))
            {
                using (var comand = connection.CreateCommand())
                {
                    comand.CommandText = $@"drop table if exists `{tableName}`;";

                    try
                    {
                        comand.ExecuteNonQuery();

                        Console.WriteLine("Dropped mysql table '{0}'", tableName);
                    }
                    catch (MySqlException exception) when (exception.SqlState == TableDoesNotExist)
                    {
                    }
                }

                connection.Complete();
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_MYSQL")
                ?? $"server=localhost; database={databaseName}; user id=mysql; password=mysql;maximum pool size=30;";
        }
    }
}