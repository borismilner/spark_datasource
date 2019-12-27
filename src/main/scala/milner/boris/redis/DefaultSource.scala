package milner.boris.redis

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Jedis

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = null

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = null

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val redisHost = parameters.getOrElse("redis_host", "127.0.0.1")
    val redisPort: Int = parameters.getOrElse("redis_port", 6379).toString.toInt
    val targetSetName = parameters.getOrElse("redis_set_key", "milner.boris")
    val redisColumn = parameters.getOrElse("redis_column_name", "error") // Throw an exception instead writing into "error"
    writeDataFrameToRedis(data, redisHost, redisPort, targetSetName, redisColumn, mode)
    null
  }

  def writeDataFrameToRedis(data: DataFrame,
                            redisHost: String,
                            redisPort: Int,
                            targetSetName: String,
                            redisColumnName: String,
                            mode: SaveMode): Unit = {

    data.foreachPartition((rows: Iterator[Row]) => {
      val jedis = new Jedis(redisHost, redisPort)
      val pipeline = jedis.pipelined()
      rows.foreach((row: Row) => {
        pipeline.sadd(targetSetName, row.getAs[String](redisColumnName))
      })
      pipeline.sync()
    })
  }
}

