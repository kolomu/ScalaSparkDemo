object SimpleKafkaProduce extends App {
  println("publishing value...")
  KafkaProducer.publishSampleData("Hello World!")
}
