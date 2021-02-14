namespace Rebus.MySql
{
    /// <summary>
    /// Enumeration of error codes rather than using magic numbers. Stolen from MySqlConnector.
    /// </summary>
    public enum MySqlErrorCode
    {
        /// <summary>
        /// DatabaseCreateExists error code
        /// </summary>
        DatabaseCreateExists = 1007,

        /// <summary>
        /// BadTable error code
        /// </summary>
        BadTable = 1051,

        /// <summary>
        /// MultiplePrimaryKey error code
        /// </summary>
        MultiplePrimaryKey = 1068,

        /// <summary>
        /// LockDeadlock error code
        /// </summary>
        LockDeadlock = 1213,
    }
}
