package com.inn.garg

import com.inn.garg.HelloSpark.{countByCountry, loadSurveyDF}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class HelloSparkTest extends FunSuite with BeforeAndAfterAll{

  @transient var spark:SparkSession=_

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local").appName("HelloSparkTest").getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data Loading test") {
    val sample = loadSurveyDF(spark,"data/sample.csv")
    val count=sample.count()
    assert(count==9,"Count should be 9")
  }


  test("count by country") {
    val df1 = loadSurveyDF(spark, "data/sample.csv")
    val df2 = countByCountry(df1)
    val map = new mutable.HashMap[String, Long]
    df2.collect().foreach(e => map.put(e.getString(0), e.getLong(1)))
    assert(map("United States") == 4,"it should be 4")
    assert(map("Canada") == 2,"it should be 2")
    assert(map("United Kingdom") == 1,"it should be 1")

  }


}
