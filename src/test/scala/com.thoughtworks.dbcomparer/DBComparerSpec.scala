import com.thoughtworks.dbcomparer.config.TableConfig
import com.thoughtworks.dbcomparer.plugin.ImpalaComparerMojo
import org.scalatest._

class DBComparerSpec extends FlatSpec with Matchers{

  "Method makeQuery" should "return a select query" in {
    val uniqueCols = List("first_name", "last_name")
    val dataCols = List("age", "sex")
    val db1 = "DB1"
    val db2 = "DB2"
    val outputLimit = 1
    val tableConfig = TableConfig("person", uniqueCols, dataCols)
    val doDataComp = true
    val comparer = new ImpalaComparerMojo()

    val (selectColsAlias, query) = comparer.makeQuery(db1, db2, outputLimit, tableConfig, doDataComp)

    val expectedQuery =
      s"""
         |SELECT
         |  a.first_name as a_first_name, b.first_name as b_first_name, a.last_name as a_last_name, b.last_name as b_last_name
         |FROM
         |  DB1.person a
         |FULL OUTER JOIN
         |  DB2.person b
         |ON
         |  (a.first_name = b.first_name AND a.last_name = b.last_name)
         |WHERE (((a.first_name is null OR b.first_name is null) AND NOT (a.first_name is null AND b.first_name is null)) AND ((a.last_name is null OR b.last_name is null) AND NOT (a.last_name is null AND b.last_name is null)))
         |      AND 1=1
         |LIMIT 1
      """.stripMargin

    expectedQuery should equal(query)
    selectColsAlias should equal(List("a_first_name", "b_first_name", "a_last_name", "b_last_name"))

//    var comparer = new ImpalaComparerMojo()

  }
}