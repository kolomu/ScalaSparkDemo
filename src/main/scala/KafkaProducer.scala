import java.util.{Properties}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {
  def publishSampleData(myValue: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("group.id","something")
    val producer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord("mytopic", "mykey", myValue)
    val demo = producer.send(record)
    println("Done: " + demo.get().timestamp())

  }
}
