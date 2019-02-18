package com.simon.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

/**
  * Accumulators and Broadcast variables cannot be recovered from checkpoint in Spark Streaming
  */
object BcAcc {
  @volatile private var instanceBC: Broadcast[Any] = null
  @volatile private var instanceACC: LongAccumulator = null

  def getBC(sc: SparkContext, data: Any): Broadcast[Any] = {
    if (instanceBC == null) {
      synchronized {
        if (instanceBC == null) {
          instanceBC = sc.broadcast(data)
        }
      }
    }
    instanceBC
  }

  def getAcc(sc: SparkContext, name:String = "Accumulator"): LongAccumulator = {
    if (instanceACC == null) {
      synchronized {
        if (instanceACC == null) {
          instanceACC = sc.longAccumulator(name)
        }
      }
    }
    instanceACC
  }

}
