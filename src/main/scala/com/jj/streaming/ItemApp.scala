package com.jj.streaming

import com.jj.streaming.service.{ItemAction, ItemProcessor}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._


object ItemApp extends App {
   // Loading the default config. By default typesafe config loads application.conf
  private val applicationConf = ConfigFactory.load

  private val master = applicationConf.getString("spark.app.master")
  private val applicationName = applicationConf.getString("spark.app.name")
  private lazy val conf = new SparkConf().setMaster(master).setAppName(applicationName)
  private val kafkaParams: Map[String, String] = Map(
    "bootstrap.servers" -> applicationConf.getString("spark.kafka.bootstrap.servers"),
    "auto.offset.reset" -> applicationConf.getString("spark.kafka.auto.offset.reset"),
    "key.serializer" -> applicationConf.getString("spark.kafka.key.serializer"),
    "value.serializer" -> applicationConf.getString("spark.kafka.value.serializer"),
    "key.deserializer" -> applicationConf.getString("spark.kafka.key.deserializer"),
    "value.deserializer" -> applicationConf.getString("spark.kafka.value.deserializer")
  )
  private val inTopics: Set[String] = {
    import scala.collection.JavaConverters._
    applicationConf.getStringList("spark.kafka.topics.in").asScala.toSet
  }
  private val checkpointDirectory = applicationConf.getString("spark.kafka.checkpoint.directory")
  private val createStreamingContext = {
    val batchDuration = {
      val duration = applicationConf.getLong("spark.kafka.batch.duration.duration")
      applicationConf.getString("spark.kafka.batch.duration.type").toLowerCase match {
        case "seconds" => Seconds(duration)
        case "milliseconds" => Milliseconds(duration)
        case "minutes" => Minutes(duration)
      }
    }
    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDirectory)

    val kstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](inTopics, kafkaParams)
    )

    val kafkaSender = {
      val kafkaSenderParams = Map(
        "bootstrap.servers" -> applicationConf.getString("spark.kafka.bootstrap.servers"),
        "key.serializer" -> applicationConf.getString("spark.kafka.key.serializer"),
        "value.serializer" -> applicationConf.getString("spark.kafka.value.serializer"),
        "key.deserializer" -> applicationConf.getString("spark.kafka.key.deserializer"),
        "value.deserializer" -> applicationConf.getString("spark.kafka.value.deserializer")
      )
      ssc.sparkContext.broadcast(KafkaSender(kafkaSenderParams))
    }

    val outTopics: Set[String] = {
      import scala.collection.JavaConverters._
      applicationConf.getStringList("spark.kafka.topics.out").asScala.toSet
    }

    ItemAction.perform(ItemProcessor.process(kstream), outTopics, kafkaSender)

    ssc
  }
  private val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createStreamingContext)
  ssc.start()
  ssc.awaitTermination()
}