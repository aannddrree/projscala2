package br.com.projscala2.accept

import br.com.projscala2.rules.RuleFile
import org.apache.spark.sql.{DataFrame}

class AcceptFile {

  def accept(df : DataFrame, outputTypes : List[String], tableName : String, colmunFamily : String) : Unit = {
      new RuleFile().received(df, outputTypes, tableName, colmunFamily)
  }
}
