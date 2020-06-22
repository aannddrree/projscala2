package br.com.projscala2

import br.com.projscala2.received.ReceivedFile

object ApplicationIngestion {
  def main(args: Array[String]): Unit = {
    new ReceivedFile().receivedFile()
  }
}