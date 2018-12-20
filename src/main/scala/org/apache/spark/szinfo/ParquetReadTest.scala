package org.apache.spark.szinfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ParquetReadTest {

  def main(args: Array[String]): Unit = {

    // Spark configurations
    val sparkConf = new SparkConf()
      .setAppName("ParquetReadTest")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.parquet(Settings.FinalDataPath)
      .as[Record]

    df.printSchema()
    df.show(20)
    val count = df.count()
    println(s"Got total $count records.")
  }

  def time[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    (ret, end - start)
  }

}

