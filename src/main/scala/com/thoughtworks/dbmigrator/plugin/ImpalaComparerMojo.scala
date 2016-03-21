package com.thoughtworks.dbmigrator.plugin

import java.sql.{DriverManager}

import org.apache.log4j.Logger
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations.Mojo


@Mojo(name = "comapare_impala")
class ImpalaComparerMojo extends AbstractMojo{
  private val logger = Logger.getLogger(classOf[ImpalaComparerMojo])

  override def execute() = {
    val jdbcUrl = "jdbc:hive2://172.18.35.27:21050/;auth=noSasl"

    val outputLimit = 100

    val db1 = "test_new_impala"
    val db2 = "achalag_impala"

    val tableName = "changelog"

    val uniqueKeyCols = List(
      "version"
    )

    val dataCols = List(
      "name"
    )

    val tableAAlias = "a"
    val tableBAlias = "b"

    val aUniqueCols = getFQColName(uniqueKeyCols)(tableAAlias)
    val bUniqueCols = getFQColName(uniqueKeyCols)(tableBAlias)

    val aDataCols = getFQColName(dataCols)(tableAAlias)
    val bDataCols = getFQColName(dataCols)(tableBAlias)

    val aNulCondGen = bUniqueCols.map(s => s"$s is null").mkString(" OR ")
    val bNulCondGen = aUniqueCols.map(s => s"$s is null").mkString(" OR ")

    val selectCols = aUniqueCols ++ bUniqueCols ++ aDataCols ++ bDataCols
    val selectColsWithAlias = selectCols.map(s => s"$s as " + s.replace('.', '_'))
    val selectColsAlias = selectCols.map(s => s.replace('.', '_'))

    val query =
      s"""
         |SELECT
         |  ${selectColsWithAlias.mkString(", ")}
         |FROM
         |  $db1.$tableName $tableAAlias
         |FULL OUTER JOIN
         |  $db2.$tableName $tableBAlias
         |ON
         |  (${makeCond(aUniqueCols,bUniqueCols)(" = ", " AND ")})
         |WHERE $aNulCondGen OR $bNulCondGen OR ${makeCond(aDataCols,bDataCols)(" <> ", " OR ")}
         |LIMIT $outputLimit
      """.stripMargin

    println(query)

    using(createConnection(jdbcUrl, "", "")) {
      connection =>
      using(connection.createStatement) {
        statement => using(statement.executeQuery(query)) {
          result =>
            logger.info("Fetching columns value")
            println(tableName)
            println(selectColsAlias)
            while (result.next) {
              println(selectColsAlias.map(col => result.getObject(col)))
            }
        }
      }
    }
  }

  def getFQColName(cols : List[String])(alias : String) = {
    cols.map(col => alias + "." + col)
  }

  def makeCond(tableACol: List[String], tableBCol: List[String])(operator:String, separator:String) = {
    tableACol.zip(tableBCol)
      .map(tuple => tuple._1 + operator + tuple._2)
      .mkString(separator)
  }

  def createConnection(url:String, username:String, password:String) = {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      DriverManager.getConnection(url, username, password)
    }
    catch {
      case e: Any => {
        logger.error("Unable to connect to database")
        throw new IllegalArgumentException("Unable to connect to database: ", e)
      }
    }
  }
  def using[T <: { def close() }](resource: T)(block: T => Unit)
  {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }
}
