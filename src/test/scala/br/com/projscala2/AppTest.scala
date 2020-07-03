package br.com.projscala2

import org.junit._
import Assert._
import br.com.projscala2.constants.Constants
import org.apache.hadoop.fs.Path
import java.io.File
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobConf}
import scala.reflect.io.Directory
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._

@Test
class AppTest extends Serializable {

    @Test
    def testOK() = assertTrue(true)

  @Test
  def testInsgestionFaster() : Unit = {

    val spark = SparkSession.builder.appName(Constants.appName)
      .config("spark.master", "local").getOrCreate

    val df = spark.read.format("csv").option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .load(Constants.fileInput)

    val tableName = "tb_drinks"
    val conf =   HBaseConfiguration.create()
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

    val data = df.rdd.map { x =>
      val key = x.getString(0)
      val value = x.getString(1)
      val put = new Put(Bytes.toBytes(key))
      put.addColumn(Bytes.toBytes("tb_drinks"), Bytes.toBytes("col1"), Bytes.toBytes(value))
      (new ImmutableBytesWritable, put)
    }
    data.saveAsNewAPIHadoopDataset(conf)
  }

    @Test
    def testInsgestionFasterFile() : Unit = {

        val directoryOutput = "/dados/table/output"
        val directory = new Directory(new File(directoryOutput))
        if (directory.exists) directory.deleteRecursively()

        val spark = SparkSession.builder.appName(Constants.appName)
          .config("spark.master", "local").getOrCreate

        val df = spark.read.format("csv").option("header", "true")
                                                .option("multiline", true)
                                                .option("sep", ";")
                                                .load(Constants.fileInput)

        //val tableName = "tb_drinks"
        val conf = new JobConf()
        FileInputFormat.setInputPaths(conf, new Path("/dados/table/input"))
        FileOutputFormat.setOutputPath(conf, new Path(directoryOutput))

        //val conf =   HBaseConfiguration.create()
        //conf.set("hbase.mapred.outputtable", tableName)
        //conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")

         val data = df.rdd.map { x =>
          val key = x.getString(0)
          val value = x.getString(1)
          val put = new Put(Bytes.toBytes(key))
          put.addColumn(Bytes.toBytes("tb_drinks"), Bytes.toBytes("col1"), Bytes.toBytes(value))
            (new ImmutableBytesWritable, put)
        }
        data.saveAsNewAPIHadoopDataset(conf)
    }

  case class drinks(id : String, country:String, beer_servings: String, spirit_servings: String, wine_servings : String, total_litres_of_pure_alcohol : String) extends Serializable

  @Test
  def insertCassandraExample() : Unit = {

    val spark = SparkSession.builder.appName(Constants.appName)
      .config("spark.master", "local")
      .config("spark.cassandra.auth.username","cassandra")
      .config("spark.cassandra.auth.password","cassandra")
      .getOrCreate

    val sc = spark.sparkContext
    //val collection = sc.parallelize(Seq(drinks(UUID.randomUUID(),"teste", "teste1", "teste2", "teste3", "teste4")))
    val collection = sc.parallelize(Seq(drinks("f59c0e1a-4bc1-4701-96fb-30d0cdf1dcaa","teste", "teste1", "teste2", "teste3", "teste4")))

    collection.saveToCassandra("bdteste",
                               "tb_drinks",
                                SomeColumns("id","country", "beer_servings", "spirit_servings", "wine_servings", "total_litres_of_pure_alcohol"))
  }

  @Test
  def insertCassandraExample2() : Unit = {

    val spark = SparkSession.builder.appName(Constants.appName)
      .config("spark.master", "local")
      .config("spark.cassandra.auth.username","cassandra")
      .config("spark.cassandra.auth.password","cassandra")
      .getOrCreate

    val sc = spark.sparkContext

    val df = spark.read.format("csv").option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .load(Constants.fileInput)
      .withColumn("id",  expr("uuid()"))
      .drop("_c5")

    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "tb_drinks", "keyspace" -> "bdteste"))
      .mode(SaveMode.Append)
      .save()
  }
}