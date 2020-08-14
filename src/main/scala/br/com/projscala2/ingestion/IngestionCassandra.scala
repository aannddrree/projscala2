package br.com.projscala2.ingestion

import br.com.projscala2.config.ApplicationConfig
import br.com.projscala2.constants.Constants
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class IngestionCassandra {

  var config = new ApplicationConfig().loadConstants()

  def save(df : DataFrame, tableName: String, dataBaseName : String) : Unit = {

    val spark = SparkSession.builder.appName(Constants.appName)
      .config("spark.master", "local")
      .config("spark.cassandra.auth.username",config.getString("output-file.cassandra.user"))
      .config("spark.cassandra.auth.password",config.getString("output-file.cassandra.password"))
      .getOrCreate

    val sc = spark.sparkContext
    val columnNumber = df.columns.length - 1

    val df2 = df
      .withColumn("id",  expr("uuid()"))
      .drop("_c" + columnNumber)

    df2.show(2)

    df2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, "keyspace" -> dataBaseName))
      .mode(SaveMode.Append)
      .save()
  }
}
