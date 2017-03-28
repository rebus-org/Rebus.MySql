using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.Timeouts;
#pragma warning disable 1998

namespace Rebus.MySql.Transport
{
    /// <summary>
    /// Manages the 
    /// </summary>
    public class DisabledTimeoutManager : ITimeoutManager
    {
        /// <summary>
        /// Stores the deferred message.
        /// </summary>
        /// <param name="approximateDueTime"></param>
        /// <param name="headers"></param>
        /// <param name="body"></param>
        /// <returns></returns>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            var messageIdToPrint = headers.GetValueOrNull(Headers.MessageId) ?? "<no message ID>";

            var message =
                $"Received message with ID {messageIdToPrint} which is supposed to be deferred until {approximateDueTime} -" +
                " this is a problem, because the internal handling of deferred messages is" +
                " disabled when using MySQL as the transport layer in, which" +
                " case the native support for a specific visibility time is used...";

            throw new InvalidOperationException(message);
        }

        /// <summary>
        /// Retrieves the messages which are due to be sent.
        /// </summary>
        /// <returns></returns>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            return DueMessagesResult.Empty;
        }
    }
}