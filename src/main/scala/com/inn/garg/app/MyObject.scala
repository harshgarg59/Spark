package com.inn.garg.app

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.impl.StaticLoggerBinder

object MyObject {

  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Spark execution is started")
    //    val schemaString = "Country/Region/Group,1950,1951,1952,1953,1954,1955,1956,1957,1958,1959,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,Total,,title,overview,release_date,vote_average,vote_count,original_language,popularity"
    //    val schema = StructType(schemaString.split(",").map(fieldName ⇒ StructField(fieldName, DataTypes.StringType, true)))
    val spark = SparkSession.builder().enableHiveSupport().appName("Spark Job").master("local[2]").getOrCreate()
    val df2 = spark.read.option("inferSchema", "true").option("header", "true")
                   .csv("C:\\Users\\harsh\\Downloads\\Compressed\\New folder\\movies-tmdb-10000.csv")
    val df3 = df2.repartition(10)
    val df4 = df3.withColumnRenamed("_c0", "year")

    val data = df4.groupBy("year").count();
    data.show(false)
    //    val count= df3.rdd.getNumPartitions
    scala.io.StdIn.readLine()
    spark.stop()
  }
}
