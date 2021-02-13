using System;
using Rebus.Time;

namespace Rebus.MySql.Tests
{
    class FakeRebusTime : IRebusTime
    {
        Func<DateTimeOffset> _nowFactory = () => DateTimeOffset.Now;

        public DateTimeOffset Now => _nowFactory();

        public void SetNow(DateTimeOffset fakeTime) => _nowFactory = () => fakeTime;
    }
}
