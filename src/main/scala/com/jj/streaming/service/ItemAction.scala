package com.jj.streaming.service

import com.jj.log.Logging
import com.jj.model.Item
import com.jj.streaming.KafkaSender
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json
import java.io.Serializable

trait Action[T, R] extends Serializable with Logging {
  def perform(stream: DStream[T], topics: Set[String], broadcast: Broadcast[R]): Unit = stream.foreachRDD(rdd => performRdd(rdd, topics, broadcast))

  def performRdd(rdd: RDD[T], topics: Set[String], kafkaSender: Broadcast[R]): Unit
}

object ItemAction extends Action[Item, KafkaSender] {

  override def performRdd(rdd: RDD[Item], topics: Set[String], broadcast: Broadcast[KafkaSender]): Unit = rdd.foreach { item =>
    topics.foreach { topic =>
      log.info(s"Sending item to Kafka topic, $topic: $item")
      broadcast.value.send(topic, Json.toJson(item).toString)
      log.info(s"Sent item to Kafka topic, $topic: $item")
    }
  }
}
