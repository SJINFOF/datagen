package org.apache.spark.szinfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.mongodb.spark._

object MongoImport {

  def main(args: Array[String]): Unit = {
    // Spark configurations
    val sparkConf = new SparkConf()
      .setAppName("MongoImport")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.mongodb.output.uri", f"${Settings.MongoUri}")
      .config("spark.mongodb.output.database", Settings.TargetMongoDbName)
      .getOrCreate()
    import spark.implicits._

    val inputDf = spark.read
      .parquet(Settings.FinalDataPath)
      .as[Record]

    MongoSpark.save(
      inputDf
        .write
        .option("collection", Settings.TargetMongoCollectionName)
        .mode("overwrite")
    )
  }
}