package com.thoughtworks.dbcomparer.plugin

import java.io.{File, FileWriter}
import java.sql.DriverManager
import com.thoughtworks.dbcomparer.config.{TableConfig, Config}
import com.typesafe.scalalogging.slf4j.Logger
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import scala.io.Source._

import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations.{Mojo, Parameter}


@Mojo(name = "comapare_impala")
class ImpalaComparerMojo extends AbstractMojo{
  implicit val formats = DefaultFormats

  @Parameter(name = "configFile")
  private var configFile: String = "/Users/achalaggarwal/Projects/DBComparer/config.json"

  @Parameter(name = "outputDir")
  private var outputDir: String = "/tmp/asd"

  val logger = Logger(LoggerFactory.getLogger(classOf[ImpalaComparerMojo]))

  override def execute() = {
    val configFile = System.getProperty("configFile", this.configFile)
    val outputDir = System.getProperty("outputDir", this.outputDir)
    val doDataComp = java.lang.Boolean.parseBoolean(System.getProperty("doDataComp", "false"))

    val config: Config = parse(
      fromFile(configFile).getLines.mkString("\n")
      ).extract[Config]

    config.tableConfigs.foreach(
      tableConfig => {
        val tuple = makeQuery(config.db1, config.db2, config.outputLimit, tableConfig, doDataComp)
        val errorRecords = executeQuery(config, selectColsAlias = tuple._1, query = tuple._2)
        val fileName = s"$outputDir/${tableConfig.tableName}"

        if (errorRecords.nonEmpty){
          logger.info(s"Found errors in ${tableConfig.tableName}")
          logger.info(s"----------------------------------------")
          writeToFile(
            fileName,
            List(
              tuple._2,
              "----------",
              tuple._1.mkString(", "),
              errorRecords.mkString("\n")
            ).mkString("\n")
          )
        } else {
          new File(fileName).delete()
        }
      }
    )
  }

  def makeQuery(db1:String, db2:String, outputLimit: Int, tableConfig: TableConfig, doDataComp:Boolean) = {
    val tableAAlias = "a"
    val tableBAlias = "b"

    val tableAAliasGen = getFQColName(tableAAlias)_
    val tableBAliasGen = getFQColName(tableBAlias)_

    val aUniqueCols = getFQColName(tableConfig.uniqueCols)(tableAAliasGen)
    val bUniqueCols = getFQColName(tableConfig.uniqueCols)(tableBAliasGen)

    val aDataCols = getFQColName(tableConfig.dataCols diff tableConfig.uniqueCols)(tableAAliasGen)
    val bDataCols = getFQColName(tableConfig.dataCols diff tableConfig.uniqueCols)(tableBAliasGen)

    val aNulCond = bUniqueCols.map(s => s"$s is null")
    val bNulCond = aUniqueCols.map(s => s"$s is null")

    val nullCond = (aNulCond zip bNulCond).map(t => xor(t._1, t._2))

    val selectCols = if (doDataComp)
      (aUniqueCols zip bUniqueCols).flatMap(t => List(t._1, t._2))
    else
      (aUniqueCols zip bUniqueCols).flatMap(t => List(t._1, t._2)) ++
      (aDataCols zip bDataCols).flatMap(t => List(t._1, t._2))

    val selectColsWithAlias = selectCols.map(s => s"$s as " + s.replace('.', '_'))
    val selectColsAlias = selectCols.map(s => s.replace('.', '_'))

    val additionCond = if (tableConfig.condition.isEmpty) "1=1" else tableConfig.condition.get.map(condition => {
      tableAAliasGen(condition.col) + " = " + condition.value
    }).mkString(" AND ")

    val query =
      s"""
         |SELECT
         |  ${selectColsWithAlias.mkString(", ")}
         |FROM
         |  $db1.${tableConfig.tableName} $tableAAlias
         |FULL OUTER JOIN
         |  $db2.${tableConfig.tableName} $tableBAlias
         |ON
         |  (${makeCond(aUniqueCols,bUniqueCols)(" = ", " AND ")})
         |WHERE (${nullCond.mkString(" AND ")})
         |      AND $additionCond
         |LIMIT $outputLimit
      """.stripMargin

    (selectColsAlias, query)
  }

  def xor(cola:String, colb:String) = s"(($cola OR $colb) AND NOT ($cola AND $colb))"

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

  def getFQColName(cols : List[String])(aliasGen : String => String) = {
    cols.map(col => aliasGen(col))
  }

  def getFQColName(alias : String)(col : String) = {
    alias + "." + col
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
