package milner.boris.redis

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Jedis

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = null

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = null

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val targetSetName = parameters.getOrElse("path", "milner.boris")
    writeDataFrameToRedis(data, targetSetName, mode)
    null
  }

  def writeDataFrameToRedis(data: DataFrame, targetSetName: String, mode: SaveMode): Unit = {
    data.foreach((row: Row) => {
      val r = new Jedis("127.0.0.1")
      r.sadd(targetSetName, row.getAs[String]("graph_node"))
    })

  }
}
