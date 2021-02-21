using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Exceptions;

#pragma warning disable 1998

namespace Rebus.MySql
{
    /// <summary>
    /// Wrapper of <see cref="MySqlConnection"/> that allows for either handling <see cref="MySqlTransaction"/> automatically, or for handling it externally
    /// </summary>
    public class DbConnectionWrapper : IDbConnection
    {
        readonly MySqlConnection _connection;
        readonly bool _managedExternally;

        MySqlTransaction _currentTransaction;
        bool _disposed;

        /// <summary>
        /// Constructs the wrapper, wrapping the given connection and transaction. It must be indicated with <paramref name="managedExternally"/> whether this wrapper
        /// should commit/rollback the transaction (depending on whether <see cref="Complete"/> is called before <see cref="Dispose()"/>), or if the transaction
        /// is handled outside of the wrapper
        /// </summary>
        public DbConnectionWrapper(MySqlConnection connection, MySqlTransaction currentTransaction, bool managedExternally)
        {
            _connection = connection;
            _currentTransaction = currentTransaction;
            _managedExternally = managedExternally;
        }

        /// <summary>
        /// Returns the current database schema
        /// </summary>
        public string Database => _connection.Database;

        /// <summary>
        /// Creates a ready to used <see cref="MySqlCommand"/>
        /// </summary>
        public MySqlCommand CreateCommand()
        {
            var sqlCommand = _connection.CreateCommand();
            sqlCommand.Transaction = _currentTransaction;
            return sqlCommand;
        }

        /// <summary>
        /// Gets the names of all the tables in the current database for the current schema
        /// </summary>
        public IEnumerable<TableName> GetTableNames()
        {
            try
            {
                return _connection.GetTableNames(_currentTransaction);
            }
            catch (MySqlException exception)
            {
                throw new RebusApplicationException(exception, "Could not get table names");
            }
        }

        /// <summary>
        /// Gets information about the columns in the table given by <paramref name="dataTableName"/>
        /// </summary>
        public Dictionary<string, string> GetColumns(string schema, string dataTableName)
        {
            try
            {
                return _connection.GetColumns(schema, dataTableName, _currentTransaction);
            }
            catch (MySqlException exception)
            {
                throw new RebusApplicationException(exception, "Could not get column names");
            }
        }

        /// <summary>
        /// Gets information about the indexes in the table given by [<paramref name="schema"/>].[<paramref name="dataTableName"/>]
        /// </summary>
        public Dictionary<string, string> GetIndexes(string schema, string dataTableName)
        {
            try
            {
                return _connection.GetIndexes(schema, dataTableName, _currentTransaction);
            }
            catch (MySqlException exception)
            {
                throw new RebusApplicationException(exception, "Could not get index names");
            }
        }

        /// <summary>
        /// Execute multiple commands separately
        /// </summary>
        /// <param name="sqlCommands">SQL commands to run separated by ---- characters</param>
        public void ExecuteCommands(string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = CreateCommand())
                {
                    try
                    {
                        command.CommandText = sqlCommand;
                        command.ExecuteNonQuery();
                    }
                    catch (MySqlException exception)
                    {
                        throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
                    }
                }
            }
        }

        /// <summary>
        /// Marks that all work has been successfully done and the <see cref="MySqlConnection"/> may have its transaction committed or whatever is natural to do at this time
        /// </summary>
        public void Complete()
        {
            if (_managedExternally) return;

            if (_currentTransaction != null)
            {
                using (_currentTransaction)
                {
                    _currentTransaction.Commit();
                    _currentTransaction.Dispose();
                    _currentTransaction = null;
                }
            }
        }

        /// <summary>
        /// Marks that all work has been successfully done and the <see cref="MySqlConnection"/> may have its transaction committed or whatever is natural to do at this time
        /// </summary>
        public async Task CompleteAsync()
        {
            if (_managedExternally) return;

            if (_currentTransaction != null)
            {
                using (_currentTransaction)
                {
                    await _currentTransaction.CommitAsync();
                    _currentTransaction.Dispose();
                    _currentTransaction = null;
                }
            }
        }

        /// <summary>
        /// Finishes the transaction and disposes the connection in order to return it to the connection pool. If the transaction
        /// has not been committed (by calling <see cref="Complete"/>), the transaction will be rolled back.
        /// </summary>
        public void Dispose()
        {
            if (_managedExternally) return;
            if (_disposed) return;

            try
            {
                try
                {
                    if (_currentTransaction != null)
                    {
                        using (_currentTransaction)
                        {
                            try
                            {
                                _currentTransaction.Rollback();
                            }
                            catch
                            {
                                // ignored
                            }

                            _currentTransaction.Dispose();
                            _currentTransaction = null;
                        }
                    }
                }
                finally
                {
                    _connection.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
