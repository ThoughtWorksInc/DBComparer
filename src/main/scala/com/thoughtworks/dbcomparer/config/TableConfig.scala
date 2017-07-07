package com.thoughtworks.dbcomparer.config

case class Condition(col:String, value:String)
case class TableConfig(tableName:String, uniqueCols:List[String], dataCols:List[String], condition:Option[List[Condition]] = None)
