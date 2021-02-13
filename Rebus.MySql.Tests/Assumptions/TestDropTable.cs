using NUnit.Framework;

namespace Rebus.MySql.Tests.Assumptions
{
    [TestFixture, Category(Categories.MySql)]
    [Ignore("run if you must")]
    public class TestDropTable
    {
        [Test]
        public void DropTableThatDoesNotExist()
        {
            MySqlTestHelper.DropTable("bimse");
        }
    }
}
