package br.com.projscala2.rules

import br.com.projscala2.ingestion.IngestionFile
import org.apache.spark.sql.DataFrame

class RuleFile {

  def received (df : DataFrame) : Unit = {
    new IngestionFile().ingestion(df)
  }
}
