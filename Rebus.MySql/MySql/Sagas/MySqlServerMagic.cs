using Rebus.Sagas;

namespace Rebus.MySql.Sagas
{
    static class MySqlServerMagic
    {
        /// <summary>
        /// Error code that is emitted on PK violations
        /// https://dev.mysql.com/doc/refman/5.7/en/error-messages-server.html
        /// </summary>
        public const int PrimaryKeyViolationNumber = 1062;

    }

}