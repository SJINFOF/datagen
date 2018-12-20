package org.apache.spark.szinfo

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HBaseImport {

  def main(args: Array[String]): Unit = {
    // Spark configurations
    val sparkConf = new SparkConf()
      .setAppName("HBaseImport")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    // HBase configurations
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "20005")
    val connection = ConnectionFactory.createConnection(conf)
    val tableName = TableName.valueOf(Settings.TargetHBaseTableName)
    val admin = connection.getAdmin
    if (Settings.DropHBaseTableBeforeImport) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, Settings.TargetHBaseTableName)

    // Read the parquet file into a DataFrame
    val inputDf = spark.read
      .parquet(Settings.FinalDataPath)
      .as[Record]

    // Import DataFrame to HBase
    val rdd = inputDf.toJSON.rdd.map(
      jsonRow => {
        val put = new Put(Bytes.toBytes(jsonRow.slice(22, 47)))
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("raw"), Bytes.toBytes(jsonRow))
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
        (new ImmutableBytesWritable, put)
      }
    )
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}