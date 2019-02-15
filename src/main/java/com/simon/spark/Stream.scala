package com.simon.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.{List => JList, Locale, Map => JMap, Set => JSet, ArrayList => JArrayList, HashMap => JHashMap}
import java.lang.{Integer => JInt, Long => JLong, Number => JNumber}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.{TopicPartition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

object Stream {

  /**
  *  Kafka client
  *  - get offset from specific timestamp.
  */
  def getOffsetFromTime(props: Properties, topicStr:String, fetchDataTime: Long, df: SimpleDateFormat): Map[TopicAndPartition, Long] ={
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      val consumer = new KafkaConsumer(props)
      try {
          val partitionInfos = consumer.partitionsFor(topicStr)
          val topicPartitions = new JArrayList[TopicPartition]
          val timestampsToSearch = new JHashMap[TopicPartition, Long]

          for(partitionInfo <-partitionInfos) {
              topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchDataTime)
          }
          //consumer.assign(topicPartitions)

          val m = consumer.offsetsForTimes(timestampsToSearch.asInstanceOf[JMap[TopicPartition, JLong]])
          var  offsetTimestamp: OffsetAndTimestamp = null
          var  tpp: TopicPartition = null
          println("\nCapture time offsets:")
          for( entry <- m ) {
              tpp = entry._1
              offsetTimestamp = entry._2
              if(offsetTimestamp != null) {
                  val partition = tpp.partition()
                  val topic = tpp.topic()
                  val  timestamp = offsetTimestamp.timestamp()
                  val  offset = offsetTimestamp.offset()
                  println("partition = " + partition +
                          ", time = " + df.format(new Date(timestamp))+
                          ", offset = " + offset)
                  // Needn't below stuff, we'll use spark to consume rather than consumer client api directly
                  // consumer.seek(entry._1, offset)
                val tpAndP = TopicAndPartition(topic, partition)
                fromOffsets += (tpAndP-> offset)
              }
          }
          println("Done\n")
          } catch {
          case e:Exception => e.printStackTrace()
          } finally {
        consumer.close()
      }
    fromOffsets
  }


  def main(args: Array[String]): Unit = {
    val topics = "simonT1"
    val brokers = "localhost:9092"
    val seconds = "15"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers)

//    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
//    if (!log4jInitialized) {
//      Logger.getRootLogger.setLevel(Level.WARN)
//    }

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

    val fromOffsets = getOffsetFromTime(props, topics, fetchDataTime, df)
    val untilOffsets = getOffsetFromTime(props, topics, untilDataTime, df)

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

    val ssc =  new StreamingContext(sc, Seconds(seconds.toInt))
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic + "#" + mmd.partition+"#"+mmd.offset.toString, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    kafkaStream.transform(rdd =>{
      rdd.filter( (line)=> {
        val key =  line._1
        var res = false
        if (key.contains("#")){
          val Array(topic, partition, offset) = key.split("#")
          val tpAndP = TopicAndPartition(topic, partition.trim.toInt)
          val untilOffset = untilOffsets.getOrElse(tpAndP, 0l)
          res = offset.trim.toLong <= untilOffset
        }
        res
      }).map(line => s"key=${line._1},value=${line._2}")
    }).foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    //ssc.awaitTermination()   // Terminate the Streaming program until manually stopped or exception occurs
    try{
      println(s"before terminate: ${df.format(new Date)}")
      ssc.awaitTerminationOrTimeout(60l * 1000)   // Terminate the Streaming program in 30s
    }finally {
      println(s"after terminate: ${df.format(new Date)}")
    }

  }
}
