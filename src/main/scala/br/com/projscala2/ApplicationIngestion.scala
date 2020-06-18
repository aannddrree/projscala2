package br.com.projscala2

import org.apache.spark.sql.{SaveMode, SparkSession}
import br.com.projscala2.constants.Directory

object ApplicationIngestion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val df = spark.read.format("csv")
                       .option("header", "true")
                       .option("multiline", true)
                       .option("sep", ";")
                       .load("DadosDrinks.csv")

    df.show()
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).parquet(Directory.dirParquet)
  }
}
