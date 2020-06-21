package br.com.projscala2.ingestion

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin}
import org.slf4j.{Logger, LoggerFactory}

class IngestionHBase {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def saveHbase(df : DataFrame, tableName : String, columnFamily : String) : Unit = {
      createTable(tableName, columnFamily)
  }

  def createTable(tableName : String, columnFamily: String) : Unit = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val table = new HTableDescriptor(TableName.valueOf(tableName))
    val family = new HColumnDescriptor(columnFamily.getBytes)

    table.addFamily(family)

    if (!admin.tableExists(table.getTableName)){
        admin.createTable(table)
        logger.info("Table created!")
    }
    else
        logger.info("Table already exists!")
  }
}
