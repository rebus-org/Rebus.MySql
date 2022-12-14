using System;
using Rebus.MySql.Transport;

namespace Rebus.Config;

/// <summary>
/// Provides extensions for managing <seealso cref="MySqlTransportOptions"/>
/// </summary>
public static class MySqlTransportOptionsExtensions
{
    /// <summary>
    /// Flags the transport as only being used for sending
    /// </summary>
    public static TTransportOptions AsOneWayClient<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlTransportOptions
    {
        options.InputQueueName = null;
        return options;
    }

    /// <summary>
    /// Configures the transport to read from <paramref name="inputQueueName"/>
    /// </summary>
    public static TTransportOptions ReadFrom<TTransportOptions>(this TTransportOptions options, string inputQueueName) where TTransportOptions : MySqlTransportOptions
    {
        options.InputQueueName = inputQueueName;
        return options;
    }

    /// <summary>
    /// Opts the client out of any table creation
    /// </summary>
    public static TTransportOptions OptOutOfTableCreation<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlTransportOptions
    {
        options.EnsureTablesAreCreated = false;
        return options;
    }

    /// <summary>
    /// Sets if table creation is allowed
    /// </summary>
    public static TTransportOptions SetEnsureTablesAreCreated<TTransportOptions>(this TTransportOptions options, bool ensureTablesAreCreated) where TTransportOptions : MySqlTransportOptions
    {
        options.EnsureTablesAreCreated = ensureTablesAreCreated;
        return options;
    }

    /// <summary>
    /// Sets if table will be dropped automatically
    /// </summary>
    public static TTransportOptions SetAutoDeleteQueue<TTransportOptions>(this TTransportOptions options, bool autoDeleteQueue) where TTransportOptions : MySqlTransportOptions
    {
        options.AutoDeleteQueue = autoDeleteQueue;
        return options;
    }

    /// <summary>
    /// Sets the delay between executions of the background cleanup task
    /// </summary>
    public static TTransportOptions SetExpiredMessagesCleanupInterval<TTransportOptions>(this TTransportOptions options, TimeSpan interval) where TTransportOptions : MySqlTransportOptions
    {
        options.ExpiredMessagesCleanupInterval = interval;
        return options;
    }

    /// <summary>
    /// If <c>null</c> will default to <seealso cref="MySqlTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
    /// </summary>
    public static TTransportOptions SetLeaseInterval<TTransportOptions>(this TTransportOptions options, TimeSpan? leaseInterval) where TTransportOptions : MySqlTransportOptions
    {
        options.LeaseInterval = leaseInterval;
        return options;
    }

    /// <summary>
    /// If <c>null</c> will default to <seealso cref="MySqlTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
    /// </summary>
    public static TTransportOptions SetLeaseTolerance<TTransportOptions>(this TTransportOptions options, TimeSpan? leaseTolerance) where TTransportOptions : MySqlTransportOptions
    {
        options.LeaseTolerance = leaseTolerance;
        return options;
    }

    /// <summary>
    /// Enables automatic lease renewal. If <paramref name="automaticLeaseRenewInterval"/> is <c>null</c> then <seealso cref="MySqlTransport.DefaultLeaseAutomaticRenewal"/> will be used instead
    /// </summary>
    public static TTransportOptions EnableAutomaticLeaseRenewal<TTransportOptions>(this TTransportOptions options, TimeSpan? automaticLeaseRenewInterval) where TTransportOptions : MySqlTransportOptions
    {
        options.LeaseAutoRenewInterval = automaticLeaseRenewInterval;
        return options;
    }

    /// <summary>
    /// Disables automatic lease renewal. Message handlers that run longer than <seealso cref="MySqlTransportOptions.LeaseInterval"/> would be processed by another worker even if the worker processing this message is healthy
    /// </summary>
    public static TTransportOptions DisableAutomaticLeaseRenewal<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MySqlTransportOptions
    {
        options.LeaseAutoRenewInterval = null;
        return options;
    }

    /// <summary>
    /// If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used
    /// </summary>
    public static TTransportOptions SetLeasedByFactory<TTransportOptions>(this TTransportOptions options, Func<string> leasedByFactory) where TTransportOptions : MySqlTransportOptions
    {
        options.LeasedByFactory = leasedByFactory;
        return options;
    }
}