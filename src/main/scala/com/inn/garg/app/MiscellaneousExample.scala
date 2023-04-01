package com.inn.garg.app

import com.inn.garg.utils.SessionProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object MiscellaneousExample extends Serializable {

  @transient val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Going to create spark session in "+this.getClass.getName)
    val spark = SessionProvider.getSparkSession(appName = "Miscellaneous Example")

    val dataList = List(
      ("Ravi", "28", "1", "2002"),
      ("Abdul", "23", "5", "81"),
      ("John", "12", "12", "6"),
      ("Rosy", "7", "8", "63"),
      ("Abdul", "23", "5", "81")
    )

    val df = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(3)
    val df1 = df.withColumn("id", monotonically_increasing_id)

    val df2=df1.withColumn("year",expr(
      """
        |case when year <21 then year+2000
        |when year <100 then year+1900
        |else year
        |end
        |""".stripMargin))
    df2.show()
  }
}
