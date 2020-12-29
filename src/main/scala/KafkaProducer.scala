import java.util.{Properties}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", classOf[StringSerializer])
  props.put("value.serializer", classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)

  def publishSampleData(myValue: String) = {
    val record = new ProducerRecord("mytopic", "mykey", myValue)
    producer.send(record)
  }
}
