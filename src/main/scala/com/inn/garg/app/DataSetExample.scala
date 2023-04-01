package com.inn.garg.app

import com.inn.garg.wrapper.Survey
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DataSetExample {

  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Dataset example").getOrCreate()
    val csvDF = spark.read.format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/sample.csv")
    import spark.implicits._
    val selDF = csvDF.select("Age", "Gender", "Country", "state").as[Survey]
    //Type Safe way
    val countryWiseCount = selDF.groupByKey(r => r.Country).count()
    //Runtime Group
    val countryWiseCount1 = selDF.groupBy("Country").count()
  }
}
