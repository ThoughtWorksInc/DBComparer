# DBComparer

Usage

`mvn com.thoughtworks.dbcomparer:dbcomparer-maven-plugin:comapare_impala -DconfigFile=config.json -DoutputDir="/tmp/output"`

config.json
```
{
  "jdbcUrl" : "jdbc:hive2://host:port/;auth=noSasl",
  "db1" : "database1",
  "db2" : "database2",
  "outputLimit" : 100,
  "tableConfigs" : [
    {
      "tableName" : "person",
      "uniqueCols" : ["first_name", "last_name"],
      "dataCols" : ["age", "sex"]
    }
  ]  
}
```
