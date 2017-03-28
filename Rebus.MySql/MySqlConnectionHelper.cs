using System.Data;
using System.Threading.Tasks;
using MySqlData = MySql.Data;

namespace Rebus.MySql
{
    /// <summary>
    /// Helps out with the construction of MySql connection objects.
    /// </summary>
    public class MySqlConnectionHelper
    {
        readonly string _connectionString;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public MySqlConnectionHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Gets a fresh, open and ready-to-use connection wrapper
        /// </summary>
        public async Task<MySqlConnection> GetConnection()
        {
            var connection = new MySqlData.MySqlClient.MySqlConnection(_connectionString);

            await connection.OpenAsync();

            var currentTransaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            return new MySqlConnection(connection, currentTransaction);
        }
    }
}