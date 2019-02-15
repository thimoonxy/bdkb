package com.simon.kafka

/**
  *  Kafka 2.0.x 之后， DefaultPartitioner采用murmur2 进行哈希，而不是hashCode
  */
object DefaultPartitioner extends App{
  for (x <- 0 until 100){
      val y = org.apache.kafka.common.utils.Utils.murmur2(Array(s"$x".toByte))
      println(s"x=$x, y=${math.abs(y)%3}")
  }

}
