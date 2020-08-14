package br.com.projscala2.rules

import br.com.projscala2.ingestion.Ingestion
import org.apache.spark.sql.DataFrame

class RuleFile {
  def received (df : DataFrame, outputTypes : List[String], tableName : String, columnFamily : String) : Unit = {
    //Ingestion Data
    outputTypes.foreach(output => new Ingestion().saveOutputData(output, df, tableName, columnFamily))
  }
}
