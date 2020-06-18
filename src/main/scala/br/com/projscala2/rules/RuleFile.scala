package br.com.projscala2.rules

import br.com.projscala2.ingestion.IngestionFile
import org.apache.spark.sql.DataFrame

object RuleFile {

  def received (df : DataFrame) : Unit = {
    IngestionFile.ingestion(df)
  }
}
