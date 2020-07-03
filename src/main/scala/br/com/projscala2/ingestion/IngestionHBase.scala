package br.com.projscala2.ingestion

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.slf4j.{Logger, LoggerFactory}


class IngestionHBase extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var i = 1

  def getConnection() : Connection = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  def saveHbase(df : DataFrame, tableName : String, columnFamily : String) : Unit = {
    createTable(tableName, columnFamily)
    saveHbaseCommon(df, tableName, columnFamily)
  }

  def saveHbaseCommon (df : DataFrame, tableName: String, columnFamily : String) : Unit ={
    val table = getConnection().getTable(TableName.valueOf(tableName))

    for (row <- df.schema.fields){
      val cols = df.select(row.name).collect()
      for (row2 <- cols){
        val put = new Put(i.toString().getBytes())
        if (row2.get(0) != null){
          put.addColumn(columnFamily.getBytes(), row.name.getBytes(), row2.get(0).toString().getBytes())
          table.put(put)
          i = i + 1
        }
      }
      i = 1
    }
  }

  def createTable(tableName : String, columnFamily: String) : Unit = {
    val admin = getConnection().getAdmin
    val table = new HTableDescriptor(TableName.valueOf(tableName))
    val family = new HColumnDescriptor(columnFamily.getBytes())
    table.addFamily(family)
    try {
        admin.createTable(table)
        logger.info("Table created!")
    } catch {
      case _: Exception => logger.info("Table already exists!")
    }
  }
}
