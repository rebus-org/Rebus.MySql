using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.DataBus;
using Rebus.Tests.Contracts;

namespace Rebus.MySql.Tests.DataBus
{
    [TestFixture]
    public class TestMySqlDataBusLazyRead : FixtureBase
    {
        IDataBusStorage _storage;
        MySqlDataBusStorageFactory _factory;

        protected override void SetUp()
        {
            _factory = new MySqlDataBusStorageFactory();
            _storage = _factory.Create();
        }

        protected override void TearDown()
        {
            _factory.CleanUp();
        }

        // Change this to a larger value if you have set up MySQL for a larger packet size
        //[TestCase(1024*1024*100)]
        [TestCase(10*1024*1024)]
        public async Task ReadingIsLazy(int byteCount)
        {
            const string dataId = "known id";

            Console.WriteLine($"Generating {byteCount/(double)(1024*1024):0.00} MB of data...");

            var data = GenerateData(byteCount);

            Console.WriteLine("Saving data...");

            await _storage.Save(dataId, new MemoryStream(data));

            Console.WriteLine("Reading data...");

            var stopwatch = Stopwatch.StartNew();
            await using var source = await _storage.Read(dataId);
            await using var destination = new MemoryStream();
            var elapsedWhenStreamIsOpen = stopwatch.Elapsed;

            Console.WriteLine($"Opening stream took {elapsedWhenStreamIsOpen.TotalSeconds:0.00} s");

            await source.CopyToAsync(destination);

            var elapsedWhenStreamHasBeenRead = stopwatch.Elapsed;

            Console.WriteLine($"Entire operation took {elapsedWhenStreamHasBeenRead.TotalSeconds:0.00} s");

            var fraction = elapsedWhenStreamHasBeenRead.TotalSeconds / 1;
            Assert.That(elapsedWhenStreamIsOpen.TotalSeconds, Is.LessThan(fraction),
                "Expected time to open stream to be less than 1/1 of the time it takes to read the entire stream");
        }

        static byte[] GenerateData(int byteCount)
        {
            var buffer = new byte[byteCount];
            new Random(DateTime.Now.GetHashCode()).NextBytes(buffer);
            return buffer;
        }
    }
}
