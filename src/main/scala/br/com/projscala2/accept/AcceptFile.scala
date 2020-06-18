package br.com.projscala2.accept

import br.com.projscala2.rules.RuleFile
import org.apache.spark.sql.{DataFrame}

object AcceptFile {

  def accept(df : DataFrame) : Unit = {
      RuleFile.received(df)
  }
}
