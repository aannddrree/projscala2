package br.com.projscala2.ingestion

import br.com.projscala2.constants.Constants
import org.apache.spark.sql.DataFrame

class Ingestion {
  def saveOutputData(outputType : String, df : DataFrame, tableName : String, columnFamily : String ) : Unit = {
    outputType match {
      case Constants.parquet => new IngestionFile().saveParquet(df)
      case Constants.avro => new IngestionFile().saveAvro(df)
      case Constants.json => new IngestionFile().saveJson(df)
      case Constants.hbase => new IngestionHBase().saveHbase(df, tableName, columnFamily)
    }
  }
}
