package com.inn.garg

import org.apache.spark.sql.SparkSession

object WordCount extends Serializable  {

  def main(args: Array[String]): Unit = {
    val sparkSession= SparkSession.builder().master("local[*]").appName("WordCount").getOrCreate()
    val rdd=sparkSession.sparkContext.textFile("C:\\WorkSpace\\SCALA_PROJECT\\Spark\\data\\sample.csv")
    val words=rdd.flatMap(k=>k.split("[\\s,\\n]"))
    val pair=words.map(m=>(m,1))
    val count=pair.reduceByKey(_+_)
    count.saveAsTextFile("C:\\WorkSpace\\SCALA_PROJECT\\Spark\\data\\output")
    System.in.read()
  }

}
