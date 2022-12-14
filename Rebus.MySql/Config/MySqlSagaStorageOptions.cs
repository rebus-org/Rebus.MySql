using System;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Sagas;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure <seealso cref="MySqlSagaStorage"/>
/// </summary>
public class MySqlSagaStorageOptions : MySqlOptions
{
    /// <summary>
    /// Creates the options with the given cnnection provider factory
    /// </summary>
    public MySqlSagaStorageOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
    {
        ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
    }

    /// <summary>
    /// Creates the options with the given <paramref name="connectionProvider"/>
    /// </summary>
    public MySqlSagaStorageOptions(IDbConnectionProvider connectionProvider)
    {
        if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));

        ConnectionProviderFactory = context => connectionProvider;
    }

    /// <summary>
    /// Creates an instance of the options via <paramref name="connectionString"/>
    /// </summary>
    public MySqlSagaStorageOptions(string connectionString, bool enlistInAmbientTransactions = false)
    {
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

        ConnectionProviderFactory = context => new DbConnectionProvider(connectionString, context.Get<IRebusLoggerFactory>(), enlistInAmbientTransactions);
    }
}