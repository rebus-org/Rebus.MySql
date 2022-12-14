using System;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.MySql;
using Rebus.MySql.DataBus;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure <seealso cref="MySqlDataBusStorage"/>
/// </summary>
public class MySqlDataBusOptions : MySqlOptions
{
    /// <summary>
    /// Creates the options with the given cnnection provider factory
    /// </summary>
    public MySqlDataBusOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
    {
        ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
    }

    /// <summary>
    /// Creates the options with the given <paramref name="connectionProvider"/>
    /// </summary>
    public MySqlDataBusOptions(IDbConnectionProvider connectionProvider)
    {
        if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));

        ConnectionProviderFactory = context => connectionProvider;
    }

    /// <summary>
    /// Creates an instance of the options via <paramref name="connectionString"/>
    /// </summary>
    public MySqlDataBusOptions(string connectionString, bool enlistInAmbientTransactions = false)
    {
        ConnectionProviderFactory = context => new DbConnectionProvider(connectionString, context.Get<IRebusLoggerFactory>(), enlistInAmbientTransactions);
    }

    /// <summary>
    /// Configures the commanbd timeout
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(240);
}