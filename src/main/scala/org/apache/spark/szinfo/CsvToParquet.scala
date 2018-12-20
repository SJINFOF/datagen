package org.apache.spark.szinfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{split, trim}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Convert raw csv files to a complete parquet file
  */
object CsvToParquet {

  val rawSchema: StructType = ScalaReflection.schemaFor[RawRecord].dataType.asInstanceOf[StructType]

  implicit class DataFrameSplitter(df: DataFrame) {
    def withColumnSplit(colName: String): DataFrame = {
      df.withColumn(colName,
        split(trim(df.col(colName)), " ").cast("array<long>"))
    }
  }

  implicit class CsvReader(spark: SparkSession) {
    def readCsv(path: String): Dataset[Record] = {
      import spark.implicits._
      // Data transformation
      val rawDf = spark
        .read
        .option("header", "true")
        .schema(CsvToParquet.rawSchema)
        .csv(path)

      val typedDf = rawDf
        .withColumnSplit("arrBidPrice")
        .withColumnSplit("arrBidOrderQty")
        .withColumnSplit("arrBidNumOrders")
        .withColumnSplit("arrBidOrders")
        .withColumnSplit("arrOfferPrice")
        .withColumnSplit("arrOfferOrderQty")
        .withColumnSplit("arrOfferNumOrders")
        .withColumnSplit("arrOfferOrders")
        .as[Record]

      typedDf
    }
  }

  def main(args: Array[String]): Unit = {
    // Spark configurations
    val sparkConf = new SparkConf()
      .setAppName("CsvToParquet")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    val fileList = List("20180103", "20180104", "20180105", "20180108", "20180109", "20180110")
    val dfList = fileList.map(x =>
      spark.readCsv(f"${Settings.DataHome}/$x/data.csv")
    )

    val finalDf = dfList.reduce((l, r) => l.union(r))
      .as[Record]
      .sort('CodeStruiDateTime)

    finalDf.show(10)
    finalDf.printSchema()
    val count = finalDf.count()
    println(f"Got $count records.")
    println(f"Writing to parquet")

    finalDf.write.parquet(Settings.IntermediateFilePath)
  }
}

