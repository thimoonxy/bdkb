package com.simon.kafka
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.{Locale, ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap, Set => JSet}
import java.lang.{Integer => JInt, Long => JLong, Number => JNumber}
import java.util.logging.Logger
import kafka.common.TopicAndPartition
import kafka.utils.Logging

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConsumerDemo extends Logging {
  def main(args: Array[String]): Unit = {
  val topics = "simonT1"
  val brokers = "localhost:9092"
  val duration = "5"  //  timeout seconds
  val maxLines = 2  // set how many lines to read in this batch
//  val assign = false  // assign or subscribe
//  val fromBeginning = false  // from begining or the latest, needs assign = true
  val setOffsetTo = "b"  // begging, end, timestamp, s: dont change offset

  val Array(assign:Boolean, fromBeginning:Boolean, fromStamp:Long) =  setOffsetTo match  {
    case "b"=> Array(true, true, 0l)    // from beginning
    case "e" => Array(true, false, 0l)  // from the end
    case "s" => Array(false, false, 0l)  // subscribe
    case t => try{val stamp = t.trim.toLong; Array(true, false, stamp)} catch {case _:Exception=> Array(false, false, 0l)}  // from timestamp
  }
  info(s"assign=${assign}, fromBeginning=${fromBeginning}")

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers)
  val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("group.id","simonG1")
      props.put("enable.auto.commit", "false")
      props.put("auto.commit.interval.ms", "1000")
      props.put("session.timeout.ms", "30000")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val now = new Date()
  val nowTime = now.getTime()

  info("Current Time: " + df.format(now))


  val consumer:KafkaConsumer[String, String] = new KafkaConsumer(props)

  if(assign){
    val pInfos = consumer.partitionsFor(topics)
    var tAndp =  new JArrayList[TopicPartition]
    for(info <- pInfos){
      val topic = info.topic()
      val partition = info.partition()
      tAndp.add(new TopicPartition(topic, partition))
    }
    consumer.assign(tAndp)
    if(fromBeginning) consumer.seekToBeginning(tAndp)
    else if (!0l.equals(fromStamp)){
      val fromOffsets = OffsetUtil.getOffsetFromTime(props, topics, fromStamp)
      for (tapt <- fromOffsets){
        val tap = tapt._1
        val offset = tapt._2
        val topic = tap.topic
        val partition = tap.partition
        val tp = new TopicPartition(topic, partition)
        consumer.seek(tp, offset)
      }
    } else consumer.seekToEnd(tAndp)
  } else {
    consumer.subscribe(topics.split(",").toSet)
  }

  try{
      val records:ConsumerRecords[String,String] = consumer.poll(duration.toLong * 1000)
      val max = records.count()
      val m = if (maxLines <= max) maxLines else max
      var c = 0
      val tpoMap = new mutable.HashMap[TopicPartition, Long]()
      if(records != null){
        for( record:ConsumerRecord[String,String] <- records){
          val key = record.key()
          val value = record.value()
          val topic = record.topic()
          val time = record.timestamp()
          val partition = record.partition()
          val offset = record.offset()
          val tp = new TopicPartition(topic, partition)
          if (tpoMap.getOrElse(tp, offset) == offset ) {
            tpoMap.put(tp, offset)  // first offset of this partition, in this batch
            consumer.seek(tp, offset)
          } // reset offset
          if(c < m) {
            consumer.seek(tp, offset+1)  // step up after consumed
            println(s"offset=${offset}, partition=${partition}, key=${key}, value=${value}, stamp=${time}, topic=${topic}")
          }
          c+=1
        }

      }
  }finally {
    consumer.commitSync()
    consumer.close()
  }




  }

}
