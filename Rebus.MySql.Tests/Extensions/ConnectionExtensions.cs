using System;
using System.Collections.Generic;
using System.Linq;
using MySqlConnector;

namespace Rebus.MySql.Tests.Extensions
{
    public static class ConnectionExtensions
    {
        public static IEnumerable<T> Query<T>(this MySqlConnection connection, string query)
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;

            var properties = typeof(T).GetProperties().Select(p => p.Name).ToArray();

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var instance = Activator.CreateInstance(typeof(T));

                foreach (var name in properties)
                {
                    var ordinal = reader.GetOrdinal(name);
                    var value = reader.GetValue(ordinal);

                    instance.GetType().GetProperty(name).SetValue(instance, value);
                }

                yield return (T)instance;
            }
        }
    }
}
