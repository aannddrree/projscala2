package br.com.projscala2.ingestion

import br.com.projscala2.constants.Constants
import org.apache.spark.sql.{DataFrame, SaveMode}

class IngestionFile {

  def ingestion (df : DataFrame) : Unit = {
    saveParquet(df)
    saveAvro(df)
  }

  def saveParquet(df : DataFrame) : Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(Constants.dirParquet)
  }

  def saveAvro(df : DataFrame) : Unit = {
    df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save(Constants.dirAvro)
  }

}
