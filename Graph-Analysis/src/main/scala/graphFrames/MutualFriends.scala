package graphFrames
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame



object MutualFriends extends App {

  val sc = new SparkContext("local[*]","Spark Graph Frames Based Mutual Friend Finder")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._


  val vertices = sqlContext.createDataFrame(List(
    ("a", "Yasas",21),
    ("b", "Bob", 27),
    ("c", "Anne",23),
    ("d", "David",19),
    ("e", "Jonny", 32),
    ("f", "Fanny",34),
    ("g", "Rock",50)
  )).toDF("id", "name", "age")

  val edges = sqlContext.createDataFrame(List(
    ("a", "b", "follow"),
    ("b", "a", "follow"),
    ("b", "c", "follow"),
    ("b", "f", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("e", "a", "friend"),
    ("d", "a", "friend"),
    ("d", "b", "friend"),
    ("a", "e", "friend")
  )).toDF("src", "dst", "relationship")

  val graph = GraphFrame(vertices,edges)


  graph.vertices.show()

//  Out going degrees of each vertices
  graph.outDegrees.show()

//  In coming degrees for each vertices
  graph.inDegrees.show()

////  Connected Vertices (Components)
//  val connectedComponents = graph.connectedComponents.run()
//  connectedComponents.select("id", "component").orderBy("component").show()


//  Can perform queries directly on vertices or edges in the graph

//  1.Get Youngest User - Method 2
  graph.vertices.createOrReplaceTempView("vertices")
  var youngestUser = sqlContext.sql("SELECT * " +
                                            "FROM vertices " +
                                            "ORDER BY age ASC LIMIT 1")
  youngestUser.show()

//  2.Get Youngest User - Method 2
  youngestUser = graph.vertices.groupBy().min("age")
  youngestUser.show()


// Filter Data
 graph.edges.filter("relationship = 'follow'").show()


//  Motif finding

//  1. Get users who have edge for both sides
  val usersEdgeForBoth  = graph.find("(user1)-[ab]->(user2); (user2)-[ba]->(user1)")
  usersEdgeForBoth.show()

//  2. Get Mutual Friends
  val mutualFriends = graph.find("(a)-[]->(b); (b)-[]->(c)").filter("a.id != c.id").dropDuplicates()

  val getMutualFriendsForTwoUsers = mutualFriends.filter("a.id == 'a' and b.id == 'b'")
  getMutualFriendsForTwoUsers.show()


//  Create a Sub Graphs
  val graph2 = graph.filterEdges("relationship = 'friend'")
    .filterVertices("age < 30")
    .dropIsolatedVertices()

  graph2.edges.show()


//  BreadthFirst Search for Graph
  val paths = graph.bfs.fromExpr("name = 'Yasas'").toExpr("age>30").edgeFilter("relationship == 'friend'").run()
  paths.show()


//  Triangle Count in the graph
  val traingles = graph.triangleCount.run()
  traingles.show()



}
