package br.com.projscala2

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ApplicationIngestion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val sc = spark.sparkContext
    val textFile = sc.textFile("README.md")

    print("teste: " + textFile.count())
  }
}
