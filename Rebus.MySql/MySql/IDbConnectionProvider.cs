using System.Threading.Tasks;
using MySqlConnector;

namespace Rebus.MySql;

/// <summary>
/// MySQL database connection provider that allows for easily changing how the current <see cref="MySqlConnection"/> is obtained,
/// possibly also changing how transactions are handled
/// </summary>
public interface IDbConnectionProvider
{
    /// <summary>
    /// Gets a wrapper with the current <see cref="MySqlConnection"/> inside
    /// </summary>
    IDbConnection GetConnection();

    /// <summary>
    /// Gets a wrapper with the current <see cref="MySqlConnection"/> inside, async version
    /// </summary>
    Task<IDbConnection> GetConnectionAsync();
}