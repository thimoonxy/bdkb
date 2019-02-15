package com.simon.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.{List => JList, Locale, Map => JMap, Set => JSet}
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


    /**
      *  Kafka client
      *  根据时间戳获取5分钟之前的offset
      */
//      var fromOffsets: Map[TopicPartition, Long] = Map()
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      val consumer = new KafkaConsumer(props)
      try {
          val partitionInfos = consumer.partitionsFor(topics)
          val topicPartitions = new java.util.ArrayList[TopicPartition]
      //    val topicPartitions = new mutable.ListBuffer[TopicPartition]
          val timestampsToSearch = new java.util.HashMap[TopicPartition, Long]
          val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val now = new Date()
          val nowTime = now.getTime()
          println("当前时间: " + df.format(now))
          val fetchDataTime = nowTime - 1000 * 60 * 5  // 计算5分钟之前的时间戳
          for(partitionInfo <-partitionInfos) {
              topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchDataTime)
          }
          consumer.assign(topicPartitions)
          // 获取每个partition一个小时之前的偏移量
          val m = consumer.offsetsForTimes(timestampsToSearch.asInstanceOf[JMap[TopicPartition, JLong]])
          var  offsetTimestamp: OffsetAndTimestamp = null
          var  tpp: TopicPartition = null
          println("开始获取各分区初始偏移量...")
          for( entry <- m ) {
              // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
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
                  // 设置读取消息的偏移量
      //            consumer.seek(entry._1, offset)
                val tpAndP = TopicAndPartition(topic, partition)
                fromOffsets += (tpAndP-> offset)
              }
          }
          println("记录各分区初始偏移量结束...")
          } catch {
          case e:Exception => e.printStackTrace()
          } finally {
        consumer.close()

      }

    /**
      * Spark Streaming
      *
      * 根据5分钟前的offset，读取Kafka
      */
    val spark = SparkSession
        .builder
        .master("local[2]")
        .appName(s"SimonStream")
        .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val ssc =  new StreamingContext(sc, Seconds(seconds.toInt))
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.partition+"#"+mmd.offset.toString, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    kafkaStream.transform(rdd =>{
      rdd.map(line => s"key=${line._1},value=${line._2}")
    }).foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()

  }
}
