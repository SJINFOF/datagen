package org.apache.spark.szinfo

object Settings {

  val Testing = false

  def DataHome: String = {
    if (Testing) {
      "file:///home/pcz/develop/datagen/data"
    } else {
      "file:///home/pengcheng/data/szinfo"
    }
  }

  val IntermediateFilePath = s"${DataHome}/tmp_data.parquet"

  val FinalDataPath = s"${DataHome}/data.parquet"

  val OverwriteExistsParquet = true


  val DropHBaseTableBeforeImport = true

  val DropMongoTableBeforeImport = true

  val TargetHBaseTableName = "hisdata30g"


  val MongoUri = "mongodb://202.120.32.216:20001"

  val TargetMongoDbName = "szinfo"

  val TargetMongoCollectionName = "hisdata30g"
}
