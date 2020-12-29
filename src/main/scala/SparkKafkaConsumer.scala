import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark with Kafka").config("spark.master", "local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    spark.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "something"
    )

    val topics = Array("mytopic")
    val kafkaRawStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    kafkaRawStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        println("RDD NICHT EMPTY!")
        rdd.map(cr => cr.value)
          .foreach(x => println(s"Juhu: ${x}"))
      } else {
        println("Keine neuen Daten...")
      }
    }) // Prints result count

    ssc.start
    ssc.awaitTermination


//    // Creating a direct Stream
//    val kafkaRawstream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](Array("mytopic"), kafkaParams)
//    )
//    // each item in kafkaRawStream is ConsumerRecord
//    val stream: DStream[(String, String)] = kafkaRawstream.map( record => (record.key, record.value))
//
//    stream.foreachRDD(rdd => {
//      if(rdd.isEmpty()) {
//        println("no new data...")
//      } else {
//        println("got something!")
//      }
//    })
//
//   /*
//    val stream1SecondWindow: DStream[(String, String)] = stream.window(Seconds(1))
//    val stream1Second: DStream[String] = stream1SecondWindow.map( f => f._2) // getting the value
//
//    import spark.implicits._
//    var dataBase = Seq.empty[(String,String)].toDF("myvalue", "secondvalue")
//    dataBase.createOrReplaceTempView("myDB")
//
//    stream1Second.foreachRDD(rdd => {
//      if(rdd.isEmpty()) {
//        println("no new data...")
//      } else {
//        println("NOT EMPTY...")
//        println(rdd)
//
//        // convert RDD to DF
//        dataBase = dataBase.union(rdd.toDF("myvalue", "secondvalue"))
//        dataBase.createOrReplaceTempView("myDB")
//
//        dataBase.show()
//      }
//    })
//
//*/
//    ssc.start()
//    ssc.awaitTermination()

  //  System.in.read

  }

}
