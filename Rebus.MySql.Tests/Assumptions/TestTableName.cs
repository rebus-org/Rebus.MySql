using NUnit.Framework;
using Rebus.MySql;

namespace Rebus.MySql.Tests.Assumptions
{
    [TestFixture]
    public class TestTableName
    {
        [TestCase("bimse", "bimse", true)]
        [TestCase("bimse", "BIMSE", true)]
        [TestCase("`bimse`", "`bimse`", true)]
        [TestCase("`bimse`", "`BIMSE`", true)]
        [TestCase("different", "bimse", false)]
        [TestCase("prefix.bimse", "`prefix`.`bimse`", true)]
        [TestCase("prefix2.bimse", "`prefix`.`bimse`", false)]
        public void CheckEquality(string name1, string name2, bool expectedToBeEqual)
        {
            var tableName1 = TableName.Parse(name1);
            var tableName2 = TableName.Parse(name2);

            var what = expectedToBeEqual
                ? Is.EqualTo(tableName2)
                : Is.Not.EqualTo(tableName2);

            Assert.That(tableName1, what);
        }

        [TestCase("table", "", "table")]
        [TestCase("`table`", "", "table")]
        [TestCase("schema.table", "schema", "table")]
        [TestCase("`schema`.`table`", "schema", "table")]
        [TestCase("`Table name with spaces in it`", "", "Table name with spaces in it")]
        [TestCase("`Table name with . in it`", "", "Table name with . in it")]
        [TestCase("`schema-qualified table name with dots in it`.`Table name with . in it`", "schema-qualified table name with dots in it", "Table name with . in it")]
        [TestCase("`Schema name with . in it`.`Table name with . in it`", "Schema name with . in it", "Table name with . in it")]
        [TestCase("`Schema name with . in it` .`Table name with . in it`", "Schema name with . in it", "Table name with . in it")]
        [TestCase("`Schema name with . in it` . `Table name with . in it`", "Schema name with . in it", "Table name with . in it")]
        [TestCase("`Schema name with . in it`. `Table name with . in it`", "Schema name with . in it", "Table name with . in it")]
        public void TableParsing(string input, string expectedSchema, string expectedTable)
        {
            var tableName = TableName.Parse(input);

            Assert.That(tableName.Schema, Is.EqualTo(expectedSchema));
            Assert.That(tableName.Name, Is.EqualTo(expectedTable));
            var expected = string.IsNullOrEmpty(expectedSchema) ? $"`{expectedTable}`" : $"`{expectedSchema}`.`{expectedTable}`";
            Assert.That(tableName.QualifiedName, Is.EqualTo(expected));
        }

        [Test]
        public void ParsesNameWithoutSchemaAssumingBlankAsDefault()
        {
            var table = TableName.Parse("TableName");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "");
            Assert.AreEqual(table.QualifiedName, "`TableName`");
        }

        [Test]
        public void ParsesBackticksNameWithoutSchemaAssumingBlankAsDefault()
        {
            var table = TableName.Parse("`TableName`");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "");
            Assert.AreEqual(table.QualifiedName, "`TableName`");
        }

        [Test]
        public void ParsesNameWithSchema()
        {
            var table = TableName.Parse("schema.TableName");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "schema");
            Assert.AreEqual(table.QualifiedName, "`schema`.`TableName`");
        }

        [Test]
        public void ParsesBackticksNameWithSchema()
        {
            var table = TableName.Parse("`schema`.`TableName`");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "schema");
            Assert.AreEqual(table.QualifiedName, "`schema`.`TableName`");
        }
    }
}
