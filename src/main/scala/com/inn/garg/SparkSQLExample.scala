package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkSQLExample {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Dataset example").getOrCreate()
    val csvDF = spark.read.format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/sample.csv")

    csvDF.createOrReplaceTempView("survey_tb1")

    spark.sql("select Country,count(*) from survey_tb1 group by Country").show()
  }
}
