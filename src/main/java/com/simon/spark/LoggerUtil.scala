package com.simon.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object LoggerUtil extends Logging {
  def setSparkLogLevels(level:Level = Level.WARN) {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for Spark" +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(level)
    }
  }
}
