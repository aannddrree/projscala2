package br.com.projscala2.ingestion

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.DataFrame

class IngestionHBaseFaster {

  def put(dt : DataFrame, columnFamily : String, tableName : String) : Unit = {
    val hConf = HBaseConfiguration.create()
    val jobConf = new JobConf(hConf, this.getClass)

   /* val data = dt.rdd.map { item =>
      val Array(key, value) = item(";")
      val rowKey = key
      val put = new Put()(Bytes.toBytes(rowKey.toString))
      put.add(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily), Bytes.toBytes(value.toString))
      (new ImmutableBytesWritable, put)
    }*/
    //data.saveAsHadoopDataset(jobConf)
  }
}
