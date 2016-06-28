package com.jj.service

import com.jj.log.Logging
import com.jj.model.Item
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json

trait Processor[T, R] extends Serializable with Logging {
  def process(stream: DStream[T]): DStream[R]
}

trait DefaultProcessor[R] extends Processor[(String, String), R]

object ItemProcessor extends DefaultProcessor[Item] {

  override def process(stream: DStream[(String, String)]): DStream[Item] = {
    stream.map(_._2).map { item =>
      log.info(s"~~~Processing: $item")
      Json.parse(item).as[Item]
    }
  }
}
