package com.ibm.twittertokafka

import java.io.{BufferedWriter, File, FileWriter}
import java.util.{Calendar, Date, Properties}

import org.apache.kafka.clients.producer._
import org.apache.log4j.Logger

object KafkaProducerRaw {
  var logger = Logger.getLogger(this.getClass.getName)
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val TOPIC = "twitter_topic"
  def sendRecordToKafka(createdAt: String, hashTag:String, message: String): Unit = {
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord(TOPIC, createdAt, hashTag+"\t"+message)
    producer.send(record)
    logger.info("Tweet created at "+createdAt+" is pushed successfully to kafka ")
    producer.close()
  }
}