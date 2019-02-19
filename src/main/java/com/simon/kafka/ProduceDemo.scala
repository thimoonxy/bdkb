package com.simon.kafka
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.Random
object ProduceDemo extends App{
  val topics = "simonT2"
  val brokers = "localhost:9092"
  val seconds = "15"
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers)
  val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val now = new Date()
  val nowTime = now.getTime()
  println("Current Time: " + df.format(now))
  val prod = new KafkaProducer[String,String](props)
  try{
    while (true){
      val key = new Random().nextInt(1000).toString
      val value = df.format(new Date().getTime)
      prod.send(new ProducerRecord[String,String](topics, key ,  value ))
      println(s"key=${key}, value=${value}")
      prod.flush()
      Thread.sleep(seconds.toInt * 1000)
    }
  }finally {
    prod.close()
  }

}
