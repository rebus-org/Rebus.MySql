﻿using System.Collections.Generic;
using System.Threading;
using Rebus.Messages;
using Rebus.MySql.Transport;
using Rebus.Transport;

namespace Rebus.MySql.Tests.Extensions;

static class TransportExtensions
{
    public static IEnumerable<TransportMessage> GetMessages(this MySqlTransport transport)
    {
        var messages = new List<TransportMessage>();

        AsyncHelpers.RunSync(async () =>
        {
            while (true)
            {
                using (var scope = new RebusTransactionScope())
                {
                    var transportMessage = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                    if (transportMessage == null) break;

                    messages.Add(transportMessage);

                    await scope.CompleteAsync();
                }
            }
        });

        return messages;
    }
}