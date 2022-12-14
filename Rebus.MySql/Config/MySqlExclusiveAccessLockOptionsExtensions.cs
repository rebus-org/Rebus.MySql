using System;
using Rebus.MySql.ExclusiveLocks;
using Rebus.MySql.Sagas;

namespace Rebus.Config;

/// <summary>
/// Provides extensions for managing <seealso cref="MySqlExclusiveAccessLock"/>
/// </summary>
public static class MySqlExclusiveAccessLockOptionsExtensions
{
    /// <summary>
    /// Opts the client out of any table creation
    /// </summary>
    public static TTransportOptions OptOutOfTableCreation<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlExclusiveAccessLockOptions
    {
        options.EnsureTablesAreCreated = false;
        return options;
    }

    /// <summary>
    /// Sets if table creation is allowed
    /// </summary>
    public static TTransportOptions SetEnsureTablesAreCreated<TTransportOptions>(this TTransportOptions options, bool ensureTablesAreCreated) where TTransportOptions : MySqlExclusiveAccessLockOptions
    {
        options.EnsureTablesAreCreated = ensureTablesAreCreated;
        return options;
    }

    /// <summary>
    /// Sets if table will be dropped automatically
    /// </summary>
    public static TTransportOptions SetAutoDeleteTable<TTransportOptions>(this TTransportOptions options, bool autoDeleteQueue) where TTransportOptions : MySqlExclusiveAccessLockOptions
    {
        options.AutoDeleteTable = autoDeleteQueue;
        return options;
    }

    /// <summary>
    /// Sets the amount of time a lock will remain in the table before it is automatically cleared out
    /// </summary>
    public static TTransportOptions SetLockExpirationTimeout<TTransportOptions>(this TTransportOptions options, TimeSpan timeout) where TTransportOptions : MySqlExclusiveAccessLockOptions
    {
        options.LockExpirationTimeout = timeout;
        return options;
    }

    /// <summary>
    /// Sets the delay between executions of the background cleanup task
    /// </summary>
    public static TTransportOptions SetExpiredLocksCleanupInterval<TTransportOptions>(this TTransportOptions options, TimeSpan interval) where TTransportOptions : MySqlExclusiveAccessLockOptions
    {
        options.ExpiredLocksCleanupInterval = interval;
        return options;
    }
}