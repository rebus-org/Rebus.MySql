using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Rebus.Bus;
using Rebus.Config;
using Rebus.ExclusiveLocks;
using Rebus.Logging;
using Rebus.Threading;
using Rebus.Time;

namespace Rebus.MySql.ExclusiveLocks;

/// <summary>
/// MySQL implementation of <see cref="IExclusiveAccessLock"/> and generic exclusive lock support
/// </summary>
public class MySqlExclusiveAccessLock : IExclusiveAccessLock, IInitializable, IDisposable
{
    /// <summary>
    /// Size of the lock_key column
    /// </summary>
    public const int LockKeyColumnSize = 255;

    /// <summary>
    /// Default amount of time a lock will remain in the table before it is automatically cleared out. Locks should
    /// always auto cleanup when they are released, but if something crashes out locks could get stuck. A lock
    /// should never remain more for than 24 hours as then something is seriously wrong.
    /// </summary>
    public static readonly TimeSpan DefaultLockExpirationTimeout = TimeSpan.FromHours(24);

    /// <summary>
    /// Default delay between executing the background cleanup task
    /// </summary>
    public static readonly TimeSpan DefaultExpiredLocksCleanupInterval = TimeSpan.FromMinutes(5);

    private readonly IDbConnectionProvider _connectionProvider;
    readonly IRebusTime _rebusTime;
    private readonly TableName _lockTableName;
    private readonly ILog _log;
    private readonly TimeSpan _lockExpirationTimeout;
    private readonly IAsyncTask _expiredLocksCleanupTask;
    private readonly bool _autoDeleteTable;
    private bool _disposed;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="connectionProvider">A <see cref="IDbConnection"/> to obtain a database connection</param>
    /// <param name="lockTableName">Name of the queue this transport is servicing</param>
    /// <param name="rebusLoggerFactory">A <seealso cref="IRebusLoggerFactory"/> for building loggers</param>
    /// <param name="asyncTaskFactory">A <seealso cref="IAsyncTaskFactory"/> for creating periodic tasks</param>
    /// <param name="rebusTime">A <seealso cref="IRebusTime"/> to provide the current time</param>
    /// <param name="options">Additional options</param>
    public MySqlExclusiveAccessLock(
        IDbConnectionProvider connectionProvider,
        string lockTableName,
        IRebusLoggerFactory rebusLoggerFactory,
        IAsyncTaskFactory asyncTaskFactory,
        IRebusTime rebusTime,
        MySqlExclusiveAccessLockOptions options)
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
        if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

        _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _lockTableName = lockTableName != null ? TableName.Parse(lockTableName) : null;
        _log = rebusLoggerFactory.GetLogger<MySqlExclusiveAccessLock>();
        _lockExpirationTimeout = options.LockExpirationTimeout ?? DefaultLockExpirationTimeout;

        var cleanupInterval = options.ExpiredLocksCleanupInterval ?? DefaultExpiredLocksCleanupInterval;
        var intervalSeconds = (int)cleanupInterval.TotalSeconds;
        _expiredLocksCleanupTask = asyncTaskFactory.Create("ExpiredLocksCleanup", PerformExpiredLocksCleanupCycle, intervalSeconds: intervalSeconds);
        _autoDeleteTable = options.AutoDeleteTable;
    }

    /// <summary>
    /// Initializes the instance
    /// </summary>
    public void Initialize()
    {
        if (_lockTableName == null) return;

        _expiredLocksCleanupTask.Start();
    }

    /// <summary>
    /// Checks if the table with the configured name exists - if not, it will be created
    /// </summary>
    public void EnsureTableIsCreated()
    {
        try
        {
            InnerEnsureTableIsCreated(_lockTableName);
        }
        catch (Exception)
        {
            // if it fails the first time, and if it's because of some kind of conflict,
            // we should run it again and see if the situation has stabilized
            InnerEnsureTableIsCreated(_lockTableName);
        }
    }

    /// <summary>
    /// Internal function to make sure the table is created
    /// </summary>
    /// <param name="table">Name of the table to create</param>
    private void InnerEnsureTableIsCreated(TableName table)
    {
        using (var connection = _connectionProvider.GetConnection())
        {
            var tableNames = connection.GetTableNames();
            if (tableNames.Contains(table))
            {
                _log.Info("Database already contains a table named {tableName} - will not create anything", table.QualifiedName);
            }
            else
            {
                _log.Info("Table {tableName} does not exist - it will be created now", table.QualifiedName);

                connection.ExecuteCommands($@"
                        CREATE TABLE {table.QualifiedName} (
                            `lock_key` varchar({LockKeyColumnSize}) NOT NULL,
                            `expiration` DATETIME NOT NULL,
                            PRIMARY KEY (`lock_key`)
                        );
                        ----
                        CREATE INDEX `idx_expiration` ON {table.QualifiedName} (
                            `expiration`
                        );");
            }

            connection.Complete();
        }
    }

    /// <summary>
    /// Checks if the table with the configured name exists - if it is, it will be dropped
    /// </summary>
    private void EnsureTableIsDropped()
    {
        try
        {
            InnerEnsureTableIsDropped(_lockTableName);
        }
        catch
        {
            // if it failed because of a collision between another thread doing the same thing, just try again once:
            InnerEnsureTableIsDropped(_lockTableName);
        }
    }

    /// <summary>
    /// Internal function to ensure the table is dropped
    /// </summary>
    /// <param name="table">Name of the table</param>
    private void InnerEnsureTableIsDropped(TableName table)
    {
        using (var connection = _connectionProvider.GetConnection())
        {
            var tableNames = connection.GetTableNames();

            if (!tableNames.Contains(table))
            {
                _log.Info("A table named {tableName} doesn't exist", table.QualifiedName);
                return;
            }

            _log.Info("Table {tableName} exists - it will be dropped now", table.QualifiedName);

            connection.ExecuteCommands($"DROP TABLE IF EXISTS {table};");
            connection.Complete();
        }
    }

    /// <summary>
    /// Acquire a lock for given key
    /// </summary>
    /// <param name="key">Locking key</param>
    /// <param name="cancellationToken">Cancellation token which will be cancelled if Rebus shuts down. Can be used if e.g. distributed locks have a timeout associated with them</param>
    /// <returns>True if the lock was acquired, false if not</returns>
    public async Task<bool> AcquireLockAsync(string key, CancellationToken cancellationToken)
    {
        using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
        {
            // See if the lock exists and bail if someone already has it
            if (await IsLockAcquiredAsync(key, cancellationToken))
            {
                return false;
            }

            // Lock does not exist, so try to insert it. Because two threads could end up here at the same
            // time, if we get a duplicate entry exception, assume someone got there before us and fail out
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                        INSERT INTO {_lockTableName.QualifiedName} (
                            lock_key,
                            expiration
                        ) VALUES (
                            @lock_key,
                            @expiration
                        )";
                command.Parameters.Add("lock_key", MySqlDbType.VarChar, LockKeyColumnSize).Value = key;
                command.Parameters.Add("expiration", MySqlDbType.DateTime).Value = _rebusTime.Now.Add(_lockExpirationTimeout);
                try
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    await connection.CompleteAsync().ConfigureAwait(false);
                }
                catch (MySqlException exception) when (exception.ErrorCode == MySqlErrorCode.DuplicateKeyEntry)
                {
                    // Someone got there before us so bail and try again later
                    return false;
                }
                return true;
            }
        }
    }

    /// <summary>
    /// Determines if a lock has been acquired or not
    /// </summary>
    /// <param name="key">Locking key</param>
    /// <param name="cancellationToken">Cancellation token which will be cancelled if Rebus shuts down. Can be used if e.g. distributed locks have a timeout associated with them</param>
    /// <returns>True of the lock was acquired already, false if not</returns>
    public async Task<bool> IsLockAcquiredAsync(string key, CancellationToken cancellationToken)
    {
        using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                        SELECT lock_key
                        FROM {_lockTableName.QualifiedName}
                        WHERE lock_key = @lock_key";
                command.Parameters.Add("lock_key", MySqlDbType.VarChar, LockKeyColumnSize).Value = key;
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    return await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Release a lock for given key
    /// </summary>
    /// <param name="key">Locking key</param>
    /// <returns>True of the lock was released, false if not</returns>
    public async Task<bool> ReleaseLockAsync(string key)
    {
        using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"DELETE FROM {_lockTableName.QualifiedName} where lock_key = @lock_key";
                command.Parameters.Add("lock_key", MySqlDbType.VarChar, LockKeyColumnSize).Value = key;
                var affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                await connection.CompleteAsync().ConfigureAwait(false);
                return affectedRows == 1;
            }
        }
    }

    /// <summary>
    /// Task called periodically to clean up expired locks if they got left over
    /// </summary>
    private async Task PerformExpiredLocksCleanupCycle()
    {
        var results = 0;
        var stopwatch = Stopwatch.StartNew();

        while (true)
        {
            using (var connection = await _connectionProvider.GetConnectionAsync().ConfigureAwait(false))
            {
                // First get a batch of up to 100 locks at a time to delete in one go
                int affectedRows = 0;
                var lockKeys = new List<string>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                            SELECT lock_key
                            FROM {_lockTableName.QualifiedName}
                            WHERE expiration < now()
                            LIMIT 100";
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            lockKeys.Add((string)reader["lock_key"]);
                        }
                    }

                    // If we have any locks to delete, clean them up in a single delete statement
                    if (lockKeys.Count > 0)
                    {
                        command.CommandText = $"DELETE FROM {_lockTableName.QualifiedName} where lock_key in ('{string.Join("','", lockKeys)}')";
                        affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }

                results += affectedRows;
                await connection.CompleteAsync().ConfigureAwait(false);

                if (affectedRows == 0) break;
            }
        }

        if (results > 0)
        {
            _log.Info("Performed expired locks cleanup in {cleanupTimeSeconds} - {expiredLockCount} expired locks were deleted",
                stopwatch.Elapsed.TotalSeconds, results);
        }
    }

    /// <summary>
    /// Shuts down the background cleanup thread
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            _expiredLocksCleanupTask.Dispose();
            if (_autoDeleteTable)
                EnsureTableIsDropped();
        }
        finally
        {
            _disposed = true;
        }
    }
}