package org.apache.spark.szinfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.{desc, concat, format_string, window}

object AddTimeSuffix {

  def main(args: Array[String]): Unit = {

    // Spark configurations
    val sparkConf = new SparkConf()
      .setAppName("AddTimeSuffix")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    // Read input DataFrame and sort by CodeStruiDateTime
    val df = spark.read.parquet(Settings.IntermediateFilePath)
      .sort("CodeStruiDateTime")

    // Add unique suffix for timestamp
    val withId = df.withColumn(
      "uniqueSuffix",
      format_string("%05d",
        row_number().over(Window.partitionBy('CodeStruiDateTime).orderBy('CodeStruiDateTime))
      )
    )

    // Concat "CodeStruiDateTime" with the unique suffix
    val finalDf = withId.withColumn(
      "CodeStruiDateTime",
      concat('CodeStruiDateTime, 'uniqueSuffix)
    ).drop("uniqueSuffix")

    // Check if all timestamps only appear once
    finalDf.groupBy('CodeStruiDateTime)
      .count()
      .sort(desc("count"))
      .show()

    // Write result DataFrame to disk
    Utils.saveAsParquet(finalDf, Settings.FinalDataPath)
  }

  def time[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    (ret, end - start)
  }

}

