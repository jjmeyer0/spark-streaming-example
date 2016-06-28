package com.jj

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

// This could probably be more generic
// Taken from:
// http://allegro.tech/2015/08/spark-kafka-integration.html
class KafkaSender(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSender {
  def apply(config: Map[String, Object]): KafkaSender = {
    val f = () => {
      import collection.JavaConversions._
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSender(f)
  }
}
