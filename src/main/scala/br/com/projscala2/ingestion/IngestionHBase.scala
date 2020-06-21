package br.com.projscala2.ingestion

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Admin}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

class IngestionHBase {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getAdmin() : Admin = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    admin
  }

  def saveHbase(df : DataFrame, tableName : String, columnFamily : String) : Unit = {
    createTable(tableName, columnFamily)
    saveAsAPI(df, tableName, columnFamily)
  }

  def saveAsAPI (df : DataFrame, tableName: String, columnFamily : String) : Unit = {
  }

  def saveHbasePut (df : DataFrame, tableName: String, columnFamily : String) : Unit ={
    /*val config = HBaseConfiguration.create
    val hTable = new HTable(config,tableName)
    */
    val data = df
    df.schema.fields.foreach(x => putData(data, x.name, tableName, columnFamily))
  }

  def putData(df : DataFrame, columnName : String, tableName: String, columnFamily : String) : Unit = {
     df.select(columnName).columns.foreach(x => insertData(x, columnName, tableName, columnFamily))
  }

  def insertData(data : String, columnName : String, tableName: String, columnFamily : String) : Unit = {
    println("columnName: " + columnName)
    println("tableName: " + tableName)
    println("columnFamily: " + columnFamily)
    println("data: " + data)
  }

  def createTable(tableName : String, columnFamily: String) : Unit = {
    val admin = getAdmin()
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
