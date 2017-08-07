package com.jj.streaming.service

import com.jj.log.Logging
import com.jj.model.Item
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import play.api.libs.json.Json

trait Processor[T, R] extends Serializable with Logging {
  def process(stream: InputDStream[T]): DStream[R]
}

trait DefaultProcessor[R] extends Processor[ConsumerRecord[String, String], R]

object ItemProcessor extends DefaultProcessor[Item] {

  override def process(stream: InputDStream[ConsumerRecord[String, String]]): DStream[Item] = {
    stream.map(_.value()).map { item =>
      log.info(s"~~~Processing: $item")
      Json.parse(item).as[Item]
    }
  }
}
