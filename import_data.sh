#!/usr/bin/env bash

function echoLine() {
    echo "-----------------------------------------------------"
}

# Convert raw csv files to a parquet file
echo "Step1: Convert csv files to parquet..."
echoLine
spark-submit  --class org.apache.spark.szinfo.CsvToParquet \
  --master "local[*]" \
  --conf spark.driver.memory=100g \
  target/datagen-1.0-SNAPSHOT-jar-with-dependencies.jar


# Add suffix to the timestamp column
echo "Step2: Add suffix to the timestamp column..."
echoLine
spark-submit  --class org.apache.spark.szinfo.AddTimeSuffix \
  --master "local[*]" \
  --conf spark.driver.memory=100g \
  target/datagen-1.0-SNAPSHOT-jar-with-dependencies.jar

# Read the data file for test
echo "Step3: Read parquet dat file for test..."
echoLine
spark-submit  --class org.apache.spark.szinfo.ParquetReadTest \
  --master "local[*]" \
  --conf spark.driver.memory=100g \
  target/datagen-1.0-SNAPSHOT-jar-with-dependencies.jar

# Import data to HBase
echo "Step4: Import data to HBase..."
echoLine
spark-submit  --class org.apache.spark.szinfo.HBaseImport \
  --master "local[*]" \
  --conf spark.driver.memory=100g \
  --conf "spark.executor.extraClassPath=`hbase classpath`" \
  target/datagen-1.0-SNAPSHOT-jar-with-dependencies.jar

# Import data to MongoDB
echo "Step4: Import data to MongoDB..."
echoLine
spark-submit  --class org.apache.spark.szinfo.MongoImport \
  --master "local[*]" \
  --conf spark.driver.memory=100g \
  target/datagen-1.0-SNAPSHOT-jar-with-dependencies.jar