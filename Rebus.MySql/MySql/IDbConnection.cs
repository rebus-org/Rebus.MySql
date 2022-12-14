using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySqlConnector;

namespace Rebus.MySql;

/// <summary>
/// Wrapper of <see cref="MySqlConnection"/> that allows for easily changing how transactions are handled, and possibly how <see cref="MySqlConnection"/> instances
/// are reused by various services
/// </summary>
public interface IDbConnection : IDisposable
{
    /// <summary>
    /// Returns the current database schema
    /// </summary>
    string Database { get; }

    /// <summary>
    /// Creates a ready to used <see cref="MySqlCommand"/>
    /// </summary>
    MySqlCommand CreateCommand();

    /// <summary>
    /// Gets the names of all the tables in the current database for the current schema
    /// </summary>
    IEnumerable<TableName> GetTableNames();

    /// <summary>
    /// Marks that all work has been successfully done and the <see cref="MySqlConnection"/> may have its transaction committed or whatever is natural to do at this time
    /// </summary>
    void Complete();

    /// <summary>
    /// Marks that all work has been successfully done and the <see cref="MySqlConnection"/> may have its transaction committed or whatever is natural to do at this time, async version
    /// </summary>
    Task CompleteAsync();

    /// <summary>
    /// Gets information about the columns in the table given by [<paramref name="schema"/>].[<paramref name="dataTableName"/>]
    /// </summary>
    Dictionary<string, string> GetColumns(string schema, string dataTableName);

    /// <summary>
    /// Gets information about the indexes in the table given by [<paramref name="schema"/>].[<paramref name="dataTableName"/>]
    /// </summary>
    Dictionary<string, string> GetIndexes(string schema, string dataTableName);

    /// <summary>
    /// Execute multiple commands separately
    /// </summary>
    /// <param name="sqlCommands">SQL commands to run separated by ---- characters</param>
    void ExecuteCommands(string sqlCommands);
}