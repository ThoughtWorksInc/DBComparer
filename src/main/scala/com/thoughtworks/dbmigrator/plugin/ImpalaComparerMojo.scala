package com.thoughtworks.dbmigrator.plugin

import java.io.FileWriter
import java.sql.DriverManager
import com.thoughtworks.dbmigrator.config.{TableConfig, Config}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source._

import org.apache.log4j.Logger
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations.{Mojo, Parameter}


@Mojo(name = "comapare_impala")
class ImpalaComparerMojo extends AbstractMojo{
  implicit val formats = DefaultFormats

  @Parameter(name = "configFile")
  private var configFile: String = "/Users/achalaggarwal/Projects/DBComparer/config.json"

  @Parameter(name = "outputDir")
  private var outputDir: String = "/tmp/asd"

  private val logger = Logger.getLogger(classOf[ImpalaComparerMojo])

  override def execute() = {
    val lines = fromFile(configFile).getLines.mkString("\n")
    val config: Config = parse(lines).extract[Config]

    config.tableConfigs.foreach(
      tableConfig => {
        val tuple = makeQuery(config.db1, config.db2, config.outputLimit, tableConfig)
        val errorRecords = executeQuery(config, selectColsAlias = tuple._1, query = tuple._2)

        if (errorRecords.nonEmpty){
          writeToFile(
            s"$outputDir/${tableConfig.tableName}",
            List(
              tuple._2,
              "----------",
              tuple._1.mkString(", "),
              errorRecords.mkString("\n")
            ).mkString("\n")
          )
        }
      }
    )
  }

  def makeQuery(db1:String, db2:String, outputLimit: Int, tableConfig: TableConfig) = {
    val tableAAlias = "a"
    val tableBAlias = "b"

    val aUniqueCols = getFQColName(tableConfig.uniqueCols)(tableAAlias)
    val bUniqueCols = getFQColName(tableConfig.uniqueCols)(tableBAlias)

    val aDataCols = getFQColName(tableConfig.dataCols)(tableAAlias)
    val bDataCols = getFQColName(tableConfig.dataCols)(tableBAlias)

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
         |  ${db1}.${tableConfig.tableName} $tableAAlias
         |FULL OUTER JOIN
         |  ${db2}.${tableConfig.tableName} $tableBAlias
         |ON
         |  (${makeCond(aUniqueCols,bUniqueCols)(" = ", " AND ")})
         |WHERE $aNulCondGen OR $bNulCondGen OR ${makeCond(aDataCols,bDataCols)(" <> ", " OR ")}
         |LIMIT ${outputLimit}
      """.stripMargin

    (selectColsAlias, query)
  }

  def executeQuery(config : Config, selectColsAlias: List[String], query : String) = {
    logger.info(query)
    var output : List[String] = List()

    using(createConnection(config.jdbcUrl, "", "")) {
      connection =>
        using(connection.createStatement) {
          statement => using(statement.executeQuery(query)) {
            result =>
              logger.info("Fetching columns value")
              while (result.next) {
                output = output ++ List(selectColsAlias.map(
                  col => {
                    val value = result.getObject(col)
                    if (value != null) value.toString else null
                  }
                ).mkString(","))
              }
          }
        }
    }
    output
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

  def writeToFile(fileName:String, data:String) =
    using (new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }
}
