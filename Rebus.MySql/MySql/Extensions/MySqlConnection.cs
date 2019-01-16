using System.Collections.Generic;

namespace Rebus.MySql.Extensions
{
    internal static class MySqlConnection
    {
        public static List<string> GetTableNames(this global::Rebus.MySql.MySqlConnection connection)
        {
            var tableNames = new List<string>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "show tables";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableNames.Add(reader[0].ToString());
                    }
                }
            }

            return tableNames;
        }
    }
}