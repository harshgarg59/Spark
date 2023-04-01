package com.inn.garg.app

import com.inn.garg.utils.SessionProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr

object DemoSpark extends Serializable {

  @transient val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SessionProvider.getSparkSession(appName = "Demo")
    val df = spark.range(1, 1000000).toDF("id").repartition(10).withColumn("squar", expr("id*id"))
                  .withColumn("name", expr("'Harsh'"))
                    df.count()
    scala.io.StdIn.readLine();
  }

}
