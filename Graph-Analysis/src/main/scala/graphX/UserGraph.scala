package graphX

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object UserGraph extends App {

  val sc = new SparkContext("local[*]","Spark GraphX Sample Application")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Seq(
      (1L, ("Shane", "Engineer")),
      (2L, ("Rex", "Driver")),
      (3L, ("Yasas", "Engineer")),
      (4L, ("John", "Teacher")),
      (5L, ("Anne", "professor")),
      (6L, ("Kit", "Teacher")),
      (7L, ("David", "Doctor"))))


  val relationships: RDD[Edge[String]] =
    sc.parallelize(Seq(
      Edge(1L, 2L, "Friend"),
      Edge(2L, 7L, "Friend"),
      Edge(1L, 5L, "Follow"),
      Edge(5L, 1L, "Friend"),
      Edge(7L, 1L, "Follow"),
      Edge(5L, 2L, "Friend"),
      Edge(3L, 7L, "Friend"),
      Edge(3L, 7L, "Friend"),
      Edge(5L, 3L, "Friend"),
      Edge(2L, 5L, "Follow"),
      Edge(5L, 7L, "Follow"),
      Edge(7L,3L,"Follow"),
      Edge(3L,5L,"Friend"),
      Edge(2L,6L,"Friend"),
      Edge(6L,3L,"Friend"),
      Edge(6L,2L,"Follow"),
      Edge(3L,2L,"Follow")))

  val defaultUser = ("Unknown", "Missing")

  val graph = Graph(users, relationships, defaultUser)

  graph.edges.collect().foreach(println)
  graph.vertices.collect().foreach(println)

  graph.inDegrees.collect().foreach(println)
  graph.outDegrees.collect().foreach(println)


  val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Engineer")
  validGraph.vertices.collect().foreach(println)

}
