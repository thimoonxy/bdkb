package com.simon.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.{Locale, ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap, Set => JSet}
import java.lang.{Integer => JInt, Long => JLong, Number => JNumber}

import scala.collection.JavaConversions._
import kafka.common.TopicAndPartition
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

object OffsetUtil extends Logging{
  /**
  *  Kafka client
  *  - get offset from specific timestamp.
  */
  def getOffsetFromTime(props: Properties, topicStr:String, fetchDataTime: Long, df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ): Map[TopicAndPartition, Long] ={
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      var oat: OffsetAndTimestamp = null
      var tp: TopicPartition = null
      val consumer = new KafkaConsumer(props)
      val timestampsToSearch = new JHashMap[TopicPartition, Long]
      info(s"\nCapture offsets for timestamp@${df.format(new Date(fetchDataTime))}:")
      try {
          val partitionInfos = consumer.partitionsFor(topicStr)
          for(pInfo <-partitionInfos) {
              timestampsToSearch.put(new TopicPartition(pInfo.topic(), pInfo.partition()), fetchDataTime)
          }
          val offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch.asInstanceOf[JMap[TopicPartition, JLong]])

          for( entry <- offsetsForTimes ) {
              tp = entry._1
              oat = entry._2
              if(oat != null) {
                  val partition = tp.partition()
                  val topic = tp.topic()
                  val timestamp = oat.timestamp()
                  val offset = oat.offset()
                  info("partition = " + partition +
                          ", time = " + df.format(new Date(timestamp))+
                          ", offset = " + offset)
                val tpAndP = TopicAndPartition(topic, partition)
                fromOffsets += (tpAndP-> offset)
              }
          }
          info("Done\n")
          } catch {
          case e:Exception => e.printStackTrace()
          } finally {
        consumer.close()
      }
    fromOffsets
  }

}
