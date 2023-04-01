package com.inn.garg.app

import com.inn.garg.utils.SessionProvider
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.StringTrimRight
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object BroadcastExampleSerializable {

  @transient val logger = Logger.getLogger(this.getClass)
  var bdData: Broadcast[Map[String, String]] = _

  def main(args: Array[String]): Unit = {

    val spark = SessionProvider.getSparkSession(appName = "BroadCast variable")
    val prdCode = spark.read.csv("data/lookup.csv").collect().map(e => e(0) -> e(1)).toMap
                       .asInstanceOf[Map[String, String]]
    val data_list = List(("98312", "2021-01-01", "1200", "01"),
      ("01056", "2021-01-02", "2345", "01"),
      ("98312", "2021-02-03", "1200", "02"),
      ("01056", "2021-02-04", "2345", "02"),
      ("02845", "2021-02-05", "9812", "02"))
    bdData = spark.sparkContext.broadcast(prdCode)
    val myfun = udf(myfun1(_:String): String)
    val df = spark.createDataFrame(data_list).toDF("code", "order_date", "price", "qty")
    spark.udf.register("my_udf", myfun)
    df.withColumn("Product", expr("my_udf(code)"))
      .show()

    scala.io.StdIn.readLine()
  }

  def myfun1(code: String): String = {
    return bdData.value.get(code).get
  }


}
