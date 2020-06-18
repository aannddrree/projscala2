package br.com.projscala2.received

import br.com.projscala2.accept.AcceptFile
import br.com.projscala2.constants.Constants
import org.apache.spark.sql.SparkSession

class ReceivedFile {

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

    AcceptFile.accept(df)
  }
}
