package com.inn.garg.app

import com.inn.garg.wrapper.Survey
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object RDDExample extends Serializable {

  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkRDD")
    val sparkContext = new SparkContext(sparkConf)
    val textRDD = sparkContext.textFile("data/sample.csv")
    val repartitionRDD = textRDD.repartition(2)
    val colRDD = repartitionRDD.filter(e => !(e.contains("Timestamp"))).map(e => e.split(",").map(_.trim))
    val surveyRDD = colRDD.map(e => Survey(e(1).toInt, e(2), e(3), e(4)))
    val filteredRDD = surveyRDD.filter(e => e.Age < 40)
    val kvRDD = filteredRDD.map(e => (e.Country, 1))
    val reducedRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)
    logger.info(reducedRDD.collect().mkString(","))

  }

}
