package org.apache.spark.szinfo

import org.apache.spark.sql.{Dataset, SaveMode}

object Utils {
  def saveAsParquet[T](df: Dataset[T], path: String): Unit = {
    if (Settings.OverwriteExistsParquet) {
      df.write.mode(SaveMode.Overwrite)
        .parquet(path)
    } else {
      df.write.parquet(path)
    }
  }
}
