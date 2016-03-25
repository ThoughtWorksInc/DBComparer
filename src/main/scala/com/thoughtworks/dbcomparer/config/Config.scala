package com.thoughtworks.dbcomparer.config

case class Config(jdbcUrl:String, db1:String, db2:String, outputLimit:Int, tableConfigs:List[TableConfig])
