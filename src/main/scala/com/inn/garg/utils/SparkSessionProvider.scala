package com.inn.garg.utils

import org.apache.spark.sql.SparkSession

object SparkSessionProvider extends Serializable {

  def getSparkSession(master: String = "local", appName: String): SparkSession = {
    SparkSession.builder().master(master).appName(appName).getOrCreate()
  }

}
