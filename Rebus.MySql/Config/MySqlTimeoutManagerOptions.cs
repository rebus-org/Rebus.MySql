using System;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.Timeouts;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure <seealso cref="MySqlTimeoutManager"/>
/// </summary>
public class MySqlTimeoutManagerOptions : MySqlOptions
{
    /// <summary>
    /// Creates the options with the given cnnection provider factory
    /// </summary>
    public MySqlTimeoutManagerOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
    {
        ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
    }

    /// <summary>
    /// Creates the options with the given <paramref name="connectionProvider"/>
    /// </summary>
    public MySqlTimeoutManagerOptions(IDbConnectionProvider connectionProvider)
    {
        if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));

        ConnectionProviderFactory = context => connectionProvider;
    }

    /// <summary>
    /// Creates an instance of the options via <paramref name="connectionString"/>
    /// </summary>
    public MySqlTimeoutManagerOptions(string connectionString, bool enlistInAmbientTransactions = false)
    {
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

        ConnectionProviderFactory = context => new DbConnectionProvider(connectionString, context.Get<IRebusLoggerFactory>(), enlistInAmbientTransactions);
    }
}