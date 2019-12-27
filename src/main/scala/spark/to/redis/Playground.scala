package spark.to.redis

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object Playground extends App {

  val spark = SparkSession.builder().appName("Playground").master("local").getOrCreate()


  val schema = List(
    StructField("_uid", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("score", IntegerType, nullable = true)
  )


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time: ${BigDecimal((t1 - t0) / 1000000000).setScale(3).toDouble} seconds.")
    result
  }

  //  var data = Seq[Row]()
  var data = new ListBuffer[Row]()

  println("Creating rows...")

  time {
    (1 to 1000000).foreach(i => {
      data += Row(i, "Person-%d".format(i), i * 100)
    })
  }

  println("Creating DF from rows...")
  val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

  println("String formatting")

  val readyDf = df.withColumn("graph_node", format_string(s"{'_uid': %d, 'name': %s, 'score': %d}", col("_uid"), col("name"), col("score")))

  println("Writing to redis")
  time {
    readyDf.write
      .options(Map(
        "redis_column_name" -> "graph_node",
        "redis_set_key" -> "AwesomePeople:nodes_ingest:Person",
        "redis_host" -> "127.0.0.1",
        "redis_port" -> "6379"
      ))
      .format("spark.to.redis")
      .save()
  }

}
