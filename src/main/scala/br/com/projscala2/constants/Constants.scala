package br.com.projscala2.constants

import br.com.projscala2.config.ApplicationConfig

object Constants {

  var config = new ApplicationConfig().loadConstants()

  val dirParquet = config.getString("constants-app.dirParquet")
  val dirAvro = config.getString("constants-app.dirAvro")
  val dirJson = config.getString("constants-app.dirJson")
  val fileInput = config.getString("constants-app.fileInput")
  val appName = config.getString("constants-app.appName")
  val avro = config.getString("constants-app.avro")
  val parquet = config.getString("constants-app.parquet")
  val hbase = config.getString("constants-app.hbase")
  val json = config.getString("constants-app.json")
  val cassandra = config.getString("constants-app.cassandra")
}
