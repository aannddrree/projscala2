package br.com.projscala2.ingestion

import br.com.projscala2.config.ApplicationConfig
import br.com.projscala2.constants.Constants
import br.com.projscala2.constants.Constants.config
import org.apache.spark.sql.DataFrame

class Ingestion {

  var config = new ApplicationConfig().loadConstants()

  def saveOutputData(outputType : String, df : DataFrame, tableName : String, columnFamily : String ) : Unit = {
    outputType match {
      case Constants.parquet => new IngestionFile().saveParquet(df)
      case Constants.avro => new IngestionFile().saveAvro(df)
      case Constants.json => new IngestionFile().saveJson(df)
      case Constants.hbase => new IngestionHBase().saveHbase(df, tableName, columnFamily)
      case Constants.cassandra => new IngestionCassandra().save(df, tableName, dataBaseName = config.getString("output-file.cassandra.database"))
    }
  }
}
