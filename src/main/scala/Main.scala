// source: https://sparkbyexamples.com/

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

object Main extends App {
  // SparkSession = entry point for Spark to use RDD/DataFrames and Datasets
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkDemo")
    .getOrCreate()


/** ----------------------------------
 *------- Setting up RDDs ------------
 * ----------------------------------- */
// RDD = Resilient Distributed Dataset, primary data abstraction like Collection in Scala but across multiple nodes.
// RDD = immutable, once created you cannot change it -> RDD consists of multiple datasets which are divided into logical partitions

  // A) Create RDD from parallelize
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd = spark.sparkContext.parallelize(dataSeq)

  // B) Create RDD from textFile
  val rdd2 = spark.sparkContext.textFile("README.md")


// With the RDD you can do TRANSFORMATIONS(only executed when action is triggered) or ACTIONS(return values from RDD to driver node)
// Transformations flatMap(), map(), reduceByKey(), filter(), sortByKey() -> all return new RDD
// Action count(), collect(), first(), max(), reduce()

/** ----------------------------------
*------- DataFrame Example  ---------
* ----------------------------------- */
  // DataFrame = distributed collection where data is organized into named columns, like a Table in a DB.
  // can be constructed with different sources (data files, tables in RDD, external db's ,etc)

  val data = Seq(
    ("James","","Smith","1991-04-01","M",3000),
    ("Michael","Rose","","2000-05-19","M",4000),
    ("Robert","","Williams","1978-09-05","M",4000),
    ("Maria","Anne","Jones","1967-12-01","F",4000),
    ("Jen","Mary","Brown","1980-02-17","F",-1)
  )

  val rdd3 = spark.sparkContext.parallelize(data)

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
  // -> IMPORTANT: You need to import the spark implicits to use .toDF
  import spark.implicits._
  val df = spark.createDataFrame(rdd3).toDF(columns:_*)

  df.show()

  // Updating columns in dataframes
  val newDf = df.withColumn("salary",col("salary")*100)
  newDf.show()

  // Append data to df
  val newRow = Seq(("Udo", "", "Maier", "1970-04-02", "M", 10000)).toDF()
  val addedDf = df.union(newRow)
  addedDf.show()
  System.in.read

  // Change data in df
  import org.apache.spark.sql.functions.{when, lower}
  val updatedDF = df.withColumn("firstname", when(lower($"firstname") === "robert", "Kevin").otherwise($"firstname"))
  updatedDF.show()

  //Convert DataFrame to RDD
  val rddFromDF = df.rdd
  rddFromDF.take(10).foreach(println)

 /** ----------------------------------
 *  -- RDD Transformation WordCount --
 * ----------------------------------- */
  val rdd2_word_splitted = rdd2.flatMap(f => f.split(" ")) // every word is now a record (before that every line was a record)
  // FlatMap: First apply the split function then flatten the RDD.
  // rdd2_word_splitted.take(10).foreach(println)

  val rdd2_word_number:RDD[(String,Int)]= rdd2_word_splitted.map(m=>(m,1))
  // rdd2_word_number.take(10).foreach(println)
  // map is used to update/drop/add column
  // RDD[String] becomes [(String,Int)]

  // filter out records which we want to inspect
  val rdd2_word_number_startingWithA = rdd2_word_number.filter(a=> a._1.startsWith("a"))
  // rdd2_word_number_startingWithA.take(10).foreach(println)

  // reduceByKey merges values for each key with the specified function
  val rdd2_countedWordsStartingWithA = rdd2_word_number_startingWithA.reduceByKey(_+_)
  // rdd2_countedWordsStartingWithA.take(10).foreach(println)

  val rdd2_resultSorted = rdd2_countedWordsStartingWithA.map(a=>(a._2,a._1)).sortByKey()
  rdd2_resultSorted.take(10).foreach(println)

  // anything that returns RDD[T] is an action (also foreach is an action)

  // count – Returns the number of records in an RDD
  println("Count : "+rdd2_resultSorted.count())

  // Action: FIRST
  val firstRec = rdd2_resultSorted.first()
  println(firstRec)

  //Action: MAX
  val datMax = rdd2_resultSorted.max()
  println("Max Record : "+datMax._1 + ","+ datMax._2)

  //Action: REDUCE
  val totalWordCount = rdd2_resultSorted.reduce((a,b) => (a._1+b._1,a._2))
  println("dataReduce Record : "+totalWordCount._1)

  // Action: TAKE
  val data3 = rdd2_resultSorted.take(3)
  data3.foreach(f=>{
    println("data3 Key:"+ f._1 +", Value:"+f._2)
  })

  //Action: COLLECT returns RDD as an Array -> You can run out of memory when using this with billions of data
  val dataArr: Array[(Int, String)] = rdd2_resultSorted.collect()
  dataArr.foreach(f=>{
    println("Key:"+ f._1 +", Value:"+f._2)
  })

  // Write RDD into a textFile
  rdd2_resultSorted.saveAsTextFile("/tmp/wordCount")

  // Shuffling is triggered by groupByKey(), reduceByKey(), join()
  // shuffle = redistribute data across executors / machines
  // shuffle is expensive (network, IO operation, etc.)

  // used to halt the app...
  System.in.read
}
