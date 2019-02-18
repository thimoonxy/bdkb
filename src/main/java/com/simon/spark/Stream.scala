package com.simon.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.simon.kafka.OffsetUtil

object Stream {
  def main(args: Array[String]): Unit = {
    LoggerUtil.setSparkLogLevels()
    val topics = "simonT1"
    val brokers = "localhost:9092"
    val seconds = "15"
    val checkpointDirectory = "/tmp/chkpt"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers)
    val props = new Properties()
        props.put("bootstrap.servers", brokers)
        props.put("group.id", "simonG1")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = new Date()
    val nowTime = now.getTime()
    println("Current Time: " + df.format(now))
    val fetchDataTime = nowTime - 1000 * 60 * 5  // Capture offsets 5min ago, as the 'from'
    val untilDataTime = nowTime - 1000 * 60 * 2  // Capture offsets 2min ago, as the 'until'

    val fromOffsets = OffsetUtil.getOffsetFromTime(props, topics, fetchDataTime, df)
    val untilOffsets = OffsetUtil.getOffsetFromTime(props, topics, untilDataTime, df)


    /**
      * Spark Streaming
      *
      * Capture data between 5min ago and 2min ago from Kafka
      */
    val spark = SparkSession
        .builder
        .master("local[2]")
        .appName(s"SimonStream")
        .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    println(s"appname=${sc.appName}, appId=${sc.applicationId}")

    val bc_untilOffsets = BcAcc.getBC(sc, untilOffsets)
    val ssc =  new StreamingContext(sc, Seconds(seconds.toInt))
    ssc.checkpoint(checkpointDirectory)   // if enable this, awaitTerminationOrTimeout seems to be malformed
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic + "#" + mmd.partition+"#"+mmd.offset.toString, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    kafkaStream.transform(rdd =>{
      rdd.filter( line=> {
        val key =  line._1
        var res = false
        if (key.contains("#")){
          val Array(topic, partition, offset) = key.split("#")
          val tpAndP = TopicAndPartition(topic, partition.trim.toInt)
//          val untilOffset = untilOffsets.getOrElse(tpAndP, 0l)
          val untilOffset = bc_untilOffsets.value.asInstanceOf[Map[TopicAndPartition, Long]].getOrElse(tpAndP, 0l)
          res = offset.trim.toLong <= untilOffset
        }
        res
      }).map(line => s"key=${line._1},value=${line._2}")
    }).foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    //ssc.awaitTermination()   // Terminate the Streaming program until manually stopped or exception occurs
    try{
      println(s"before terminate: ${df.format(new Date)}")
      ssc.awaitTerminationOrTimeout(60l * 1000)   // Terminate the Streaming program in 60s
    }finally {
      println(s"after terminate: ${df.format(new Date)}")
      try{
        ssc.stop()
        System.exit(1)
      } catch {case _:Exception=>{}}
    }

  }
}
