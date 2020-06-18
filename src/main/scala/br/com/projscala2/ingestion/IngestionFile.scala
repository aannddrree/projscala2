package br.com.projscala2.ingestion

import br.com.projscala2.constants.Constants
import org.apache.spark.sql.{DataFrame, SaveMode}

object IngestionFile {

  def ingestion (df : DataFrame) : Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(Constants.dirParquet)
  }
}
