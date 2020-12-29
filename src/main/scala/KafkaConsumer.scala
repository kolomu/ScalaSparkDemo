import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util
import java.util.Properties
import scala.collection.JavaConverters._

object KafkaConsumer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  val TOPIC = "mytopic"

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records = consumer.poll(500)
    for (record <- records.asScala) {
      println("Got record with value: " + record.value())
    }
  }
}
