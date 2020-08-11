package graphFrames
import graphFrames.MutualFriends.graph
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame


object FlightPerformance extends App {

  val sc = new SparkContext("local[*]","On-Time Flight Performance Analysis")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val airports_df = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter","\t")
    .load("D:\\Spark\\graphFrames\\data\\airport-codes-na.txt")

  airports_df.registerTempTable("airports_na")


  val departureDelays_df = sqlContext.read.format("com.databricks.spark.csv")
    .option("header","true")
    .load("D:\\Spark\\graphFrames\\data\\departuredelays.csv")

  departureDelays_df.registerTempTable("departureDelays")
  departureDelays_df.cache()


  var tripIATA = sqlContext.sql("SELECT " +
                                            "DISTINCT iata " +
                                        "FROM ( " +
                                                  "SELECT " +
                                                        "DISTINCT origin AS iata " +
                                                  "FROM departureDelays " +
                                                  "UNION ALL " +
                                                  "SELECT " +
                                                        "DISTINCT destination AS iata " +
                                                  "FROM departureDelays " +
                                                ") a")

  tripIATA.registerTempTable("tripIATA")
  tripIATA.show(10)


  var airports = sqlContext.sql("SELECT " +
                                              "f.IATA, " +
                                              "f.City, " +
                                              "f.State, " +
                                              "f.Country " +
                                        "FROM airports_na f " +
                                        "JOIN tripIATA t " +
                                        "ON t.IATA = f.IATA")

  airports.registerTempTable("airports")
  airports.cache()
  airports.show(10)


  val geoDepartures = sqlContext.sql("SELECT " +
                                                      "CAST(f.date as int) AS tripid, " +
                                                      "CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('2014-', CONCAT(CONCAT(SUBSTR(CAST(f.date AS string), 1, 2), '-')), SUBSTR(CAST(f.date AS string), 3, 2)), ' '), SUBSTR(CAST(f.date AS string), 5, 2)), ':'), SUBSTR(cast(f.date AS string), 7, 2)), ':00') AS timestamp) AS `localdate`, " +
                                                      "CAST(f.delay AS INT), " +
                                                      "CAST(f.distance AS INT), " +
                                                      "f.origin AS src, " +
                                                      "f.destination AS dst, " +
                                                      "o.city AS city_src, " +
                                                      "d.city AS city_dst, " +
                                                      "o.state AS state_src, " +
                                                      "d.state AS state_dst " +
                                              "FROM departuredelays f " +
                                              "JOIN airports o " +
                                              "ON o.iata = f.origin " +
                                              "JOIN airports d " +
                                              "ON d.iata = f.destination " +
                                              "ORDER BY tripid")

  geoDepartures.show(10)
  geoDepartures.count()

  geoDepartures.registerTempTable("geoDepartures")
  geoDepartures.cache()


//  Create Graph

  val vertices = airports.withColumnRenamed("IATA", "id").distinct()
  val edges = geoDepartures.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

  vertices.show(20)
  edges.show(20)

  val graph = GraphFrame(vertices,edges)

  val verticesCount = graph.vertices.count()
  val edgesCount = graph.edges.count()

  println(s"Vertices Count : $verticesCount")
  println(s"Edges Count : $edgesCount")

  graph.inDegrees.show()
  graph.outDegrees.show()

//  Max Delay
  graph.edges.groupBy().max("delay").show()

//  Min Delay
  graph.edges.groupBy().min("delay").show()

//  Total Number of Flights Delayed
  println("Number of Flights Delayed : "+graph.edges.filter("delay > 0").count())

//  Total Number of Early Flights
  println("Number of Flights Early : "+graph.edges.filter("delay<0").count())

//  Total number of flights on time
  println("Number of Flights on time : "+graph.edges.filter("delay=0").count())

//  Sort Incoming and Outgoing Degrees to Each Geo Location
  graph.degrees.sort(desc("degree")).show(20)



//  Motif Findings

//  1. Up and Down Trips
  val upAndDownTrips = graph.find("(a)-[ab]->(b); (b)-[ba]->(a)")
    .filter("ab.tripid = ba.tripid or ab.tripid<ba.tripid")

  upAndDownTrips.show()

//  2. Triangular trip ids with over 100 delays
  val result = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
    .filter("ab.tripid<bc.tripid and bc.tripid<ca.tripid and (ab.delay+bc.delay+ca.delay > 100)")
  result.show()



//  triangle count
  val triangles = graph.triangleCount.run()
  triangles.show()



//  Find Paths using BFS
  val sourceCity  = "Brookings"
  val destCity = "Winnipeg"
  //  Find trip routes which have lands with delay in given two cities (Max Stops 4)
  val paths = graph.bfs.fromExpr(s"f.City = '$sourceCity'").toExpr(s"f.City = '$destCity'").edgeFilter("delay>0").maxPathLength(4).run()
  paths.show()



//  Page-Rank Algorithm to determine Rankings of Each Airport
  val ranks = graph.pageRank.resetProbability(0.15).maxIter(5).run()
  ranks.vertices.orderBy(desc("pagerank")).show(20)


//  Most Popular Flights (Src and Dest)
  val tripsOrdered = graph.edges.groupBy("src", "dst").agg(count("delay").alias("Trip Count")).orderBy(desc("Trip Count"))
  tripsOrdered.show(10)


}
