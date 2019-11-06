package milner.boris.redis

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


object Playground extends App {

  val spark = SparkSession.builder().appName("Playground").master("local").getOrCreate()


  val schema = List(
    StructField("_uid", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("score", IntegerType, nullable = true)
  )

  val data = Seq(
    Row(1, "Albert", 100),
    Row(2, "Isaac", 200),
    Row(3, "Richard", 300)
  )

  val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

  val readyDf = df.withColumn("graph_node", format_string(s"{'_uid': %d, 'name': %s, 'score': %d}", col("_uid"), col("name"), col("score")))

  readyDf.write
    .options(Map("format" -> "customFormat"))
    .format("milner.boris.redis")
    .save("AwesomePeople:nodes_ingest:Person")

}
