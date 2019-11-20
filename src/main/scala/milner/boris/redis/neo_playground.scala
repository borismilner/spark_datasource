//package milner.boris.redis
//
//import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
//import org.apache.spark.graphx.lib.PageRank
//import org.apache.spark.sql.SparkSession
//import org.neo4j.spark.Neo4j
//import org.apache.spark.sql.functions.{avg, explode}
//
//object neo_playground extends App {
//
//  val spark = SparkSession.builder().appName("Playground").master("local").getOrCreate()
//  val neo = Neo4j(spark.sparkContext)
//
//  val graphQuery = "MATCH (n:Person)-[r:ACTED_IN]->(m:Movie) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"
//  val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph
//  val statsDF = graph.collectNeighbors(EdgeDirection.Either)
//    .toDF("ID", "neighbours")
//    // Flatten neighbours column
//    .withColumn("neighbour", explode($"neighbours"))
//    // and extract neighbour id
//    .select($"ID".alias("this_id"), $"neighbour._1".alias("other_id"))
//    // join with people
//    .join(people, people("ID") === $"other_id")
//    .groupBy($"this_id")
//    .agg(avg($"gender"), avg($"income"))
//  neo.saveGraph(graph)
//}
