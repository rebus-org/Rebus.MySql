using System;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Logging;
using IsolationLevel = System.Data.IsolationLevel;

#pragma warning disable 1998

namespace Rebus.MySql;

/// <summary>
/// Implementation of <see cref="IDbConnectionProvider"/>
/// </summary>
public class DbConnectionProvider : IDbConnectionProvider
{
    readonly bool _enlistInAmbientTransaction;
    readonly string _connectionString;
    readonly ILog _log;

    /// <summary>
    /// Creates the connection provider with the given <paramref name="connectionString"/>.
    /// Will use <see cref="System.Data.IsolationLevel.RepeatableRead"/> by default on transactions,
    /// unless another isolation level is set with the <see cref="IsolationLevel"/> property
    /// </summary>
    public DbConnectionProvider(string connectionString, IRebusLoggerFactory rebusLoggerFactory, bool enlistInAmbientTransaction = false)
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

        _log = rebusLoggerFactory.GetLogger<DbConnectionProvider>();

        _connectionString = connectionString;
        _enlistInAmbientTransaction = enlistInAmbientTransaction;
    }

    /// <summary>
    /// Callback, which will be invoked every time a new connection is about to be opened
    /// </summary>
    public Func<MySqlConnection, Task> SqlConnectionOpening { get; set; } = async _ => { };

    /// <summary>
    /// Gets a nice ready-to-use database connection with an open transaction
    /// </summary>
    public IDbConnection GetConnection()
    {
        MySqlConnection connection = null;
        MySqlTransaction transaction = null;
        try
        {
            if (_enlistInAmbientTransaction == false)
            {
                connection = CreateSqlConnectionSuppressingAPossibleAmbientTransaction();
                transaction = connection.BeginTransaction(IsolationLevel);
            }
            else
            {
                connection = CreateSqlConnectionInAPossiblyAmbientTransaction();
            }

            return new DbConnectionWrapper(connection, transaction, false);
        }
        catch (Exception)
        {
            connection?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Gets a wrapper with the current <see cref="MySqlConnection"/> inside, async version
    /// </summary>
    public async Task<IDbConnection> GetConnectionAsync()
    {
        MySqlConnection connection = null;
        MySqlTransaction transaction = null;
        try
        {
            if (_enlistInAmbientTransaction == false)
            {
                connection = CreateSqlConnectionSuppressingAPossibleAmbientTransaction();
                transaction = await connection.BeginTransactionAsync(IsolationLevel);
            }
            else
            {
                connection = CreateSqlConnectionInAPossiblyAmbientTransaction();
            }

            return new DbConnectionWrapper(connection, transaction, false);
        }
        catch (Exception)
        {
            connection?.Dispose();
            throw;
        }
    }

    MySqlConnection CreateSqlConnectionInAPossiblyAmbientTransaction()
    {
        var connection = new MySqlConnection(_connectionString);

        if (SqlConnectionOpening != null)
        {
            AsyncHelpers.RunSync(() => SqlConnectionOpening(connection));
        }

        // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
        connection.Open();

        var transaction = System.Transactions.Transaction.Current;
        if (transaction != null)
        {
            connection.EnlistTransaction(transaction);
        }

        return connection;
    }

    MySqlConnection CreateSqlConnectionSuppressingAPossibleAmbientTransaction()
    {
        using (new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
        {
            var connection = new MySqlConnection(_connectionString);

            if (SqlConnectionOpening != null)
            {
                AsyncHelpers.RunSync(() => SqlConnectionOpening(connection));
            }

            // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
            connection.Open();

            return connection;
        }
    }

    /// <summary>
    /// Gets/sets the isolation level used for transactions
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.RepeatableRead;
}