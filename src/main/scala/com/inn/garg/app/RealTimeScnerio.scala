package com.inn.garg.app

import com.inn.garg.utils.SessionProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, split, monotonically_increasing_id, posexplode}


object RealTimeScnerio extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SessionProvider.getSparkSession(appName = "RealTime")
    import spark.implicits._
    val df = Seq(
      (("2019,2018,2017"), ("100,200,300"), ("IN,PRE,POST")),
      (("2018"), ("73"), ("IN")),
      (("2018,2017"), ("56,89"), ("IN,PRE")))
      .toDF("Date", "Amount", "Status")
      .withColumn("idx", monotonically_increasing_id)
    df.columns.filter(_ != "idx").map {
      c => df.select($"idx", posexplode(split(col(c), ","))).withColumnRenamed("col", c)
    }

      .reduce((ds1, ds2) => ds1.join(ds2, Seq("idx", "pos")))
      .select($"Date", $"Amount", $"Status", $"pos".plus(1).as("Sequence"))
      .show

  }

}
