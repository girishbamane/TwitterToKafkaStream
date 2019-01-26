package com.ibm.twittertokafka


import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Locale

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterToKafkaStream {
  var logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val appName = "TwitterDAtaAnalysis12202018"
    val consumerKey = "wUMY7vQJ69GuIhNEsZSjbnwT5"
    val consumerSecret = "XDzw6m6ijATcFniILLZJc2A5vNHM5cvIdwnYPKVkLGO8AXjZFI"
    val accessToken = "195335747-Z5xGh7ucZgUL39K1q8PeyqmzVmKTLcBk74JZAW7S"
    val accessSecret = "r1goNJKk9NQsSJErn0XGSGEZgVmxlfQQ8zxadwknhl2If"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val configurationBuilder = new ConfigurationBuilder()
    configurationBuilder.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessSecret)
    val authenticate = new OAuthAuthorization(configurationBuilder.build())
    val stream = TwitterUtils.createStream(ssc, Some(authenticate)).filter(_.getLang() == "en").filter(_.getText.toString.contains("#"))
    stream.foreachRDD { rdd => {
      rdd.foreach { ele =>
        var hashTagEntityArray = ele.getHashtagEntities
        hashTagEntityArray.foreach { hashTag =>
          //if (isAboutApple(hashTag.getText)) {
          val formatedDate = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH)
            .parse(ele.getCreatedAt.toString))
          KafkaProducerRaw.sendRecordToKafka(formatedDate, hashTag.getText, ele.getText.replaceAll("\n", " "))
            //logger.info(s"RawTweet : $ele HashTag: ${hashTag.getText} Text : ${ele.getText}")
            println("abcd created at "+ele.getCreatedAt + " Current time "+ LocalDateTime.now())
            println("Parsed date : "+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss ").format(new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH)
            .parse(ele.getCreatedAt.toString)))
          //}
        }
      }
    }
    }

    def isAboutApple(hashTag: String): Boolean = {
      var list = List[String]("apple", "iphone", "ipad", "applewatch", "ipod", "ios", "ilife")
      if (list.contains(hashTag.toLowerCase()))
        true
      else false
    }

    ssc.start()
    ssc.awaitTermination()
  }

}