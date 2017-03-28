using System.Data;
using MySql.Data.MySqlClient;

namespace Rebus.MySql.Extensions
{
    internal static class MySqlCommand
    {
        public static MySqlParameter CreateParameter(this global::MySql.Data.MySqlClient.MySqlCommand command, string name, DbType dbType, object value)
        {
            var parameter = command.CreateParameter();
            parameter.DbType = dbType;
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }
    }
}