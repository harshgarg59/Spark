package com.inn.garg.app

import com.inn.garg.utils.SessionProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SparkSchemaExample extends Serializable {
  @transient val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Inside " + this.getClass)
    val spark = SessionProvider.getSparkSession(appName = "SparkWith Schema")
    val jsonDF = spark.read.format("json").option("path", "data/flight-time.json").option("dateFormat", "M/d/y").load()
    logger.info("jsonDF schema is" + jsonDF.schema.simpleString)
    jsonDF.show()


    val parquetDF = spark.read.format("parquet").option("path", "data/flight-time.parquet").load()
    logger.info("parquetDF schema is " + parquetDF.schema)
    parquetDF.show()


    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))


    val csvDF = spark.read.format("csv").option("header", "true").option("mode", "FAILFAST")
                     .option("dateFormat", "M/d/y").option("path", "data/flight-time.csv").schema(flightSchemaStruct).load()
    csvDF.show()
    spark.stop();
  }
}
