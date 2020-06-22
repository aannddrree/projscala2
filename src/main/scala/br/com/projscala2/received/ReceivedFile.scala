package br.com.projscala2.received

import br.com.projscala2.accept.AcceptFile
import br.com.projscala2.config.ApplicationConfig
import br.com.projscala2.constants.Constants
import org.apache.spark.sql.SparkSession

class ReceivedFile {

  var config = new ApplicationConfig().loadConstants()

  def receivedFile () : Unit = {

    val spark = SparkSession
      .builder()
      .appName(Constants.appName)
      .config("spark.master", "local")
      .getOrCreate();

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .load(Constants.fileInput)

    val outputTypes = getOutputTypes()

    new AcceptFile().accept(df,
                            outputTypes,
                            config.getString("table-app.tableName"),
                            config.getString("table-app.columnFamily"))
  }

  def getOutputTypes() : List[String] = {
    var outputTypes = List.empty[String]

    val parquet = config.getBoolean("output-file.parquet")
    val avro = config.getBoolean("output-file.avro")
    val hbase = config.getBoolean("output-file.hbase")

    if (parquet)
      outputTypes :+= config.getString("constants-app.parquet")
    if (avro)
      outputTypes :+= config.getString("constants-app.avro")
    if (hbase)
      outputTypes :+= config.getString("constants-app.hbase")

    outputTypes
  }
}
