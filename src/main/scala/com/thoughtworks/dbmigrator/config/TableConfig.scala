package com.thoughtworks.dbmigrator.config

case class TableConfig(tableName:String, uniqueCols:List[String], dataCols:List[String]) {

}
