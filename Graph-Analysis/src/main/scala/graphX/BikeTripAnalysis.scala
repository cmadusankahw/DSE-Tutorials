package graphX

import graphFrames.FlightPerformance.sqlContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BikeTripAnalysis extends App {

  val sc = new SparkContext("local[*]","Bike Trip Analysis")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._


  val station_df = sqlContext.read.format("com.databricks.spark.csv")
    .option("header","true")
    .load("D:\\Spark\\graphFrames\\data\\station.csv")

  val trip_df = sqlContext.read.format("com.databricks.spark.csv")
    .option("header","true")
    .load("D:\\Spark\\graphFrames\\data\\trip.csv")


  station_df.createOrReplaceTempView("station_csv")
  trip_df.createOrReplaceTempView("trip_csv")


  station_df.printSchema()
  trip_df.printSchema()

  val stationsAndNames = station_df
    .selectExpr("float(id) as station_id", "name")
    .distinct()

  val stations = trip_df
    .select("start_station_id").withColumnRenamed("start_station_id", "station_id")
    .union(trip_df.select("end_station_id").withColumnRenamed("end_station_id", "station_id"))
    .distinct()
    .select(col("station_id").cast("long").alias("value"))

  stations.show(5)

//  Create Vertices (Stations id and Name Placed in Vertices)
  val vertices:RDD[(VertexId, String)] = stations
    .join(stationsAndNames, stations("value") === stationsAndNames("station_id"))
    .select(col("station_id").cast("long"), col("name"))
    .rdd
    .map(row => (row.getLong(0), row.getString(1)))


  vertices.take(10).foreach(println)


  val edges:RDD[Edge[Long]] = trip_df
    .select(col("start_station_id").cast("long"), col("end_station_id").cast("long"))
    .rdd
    .map(row => Edge(row.getLong(0), row.getLong(1),1))

  edges.take(10).foreach(println)

  val defaultStation = "Unknown Location"

  val graph = Graph(vertices,edges,defaultStation)

  println("Total Number of Vertices : "+graph.numVertices)
  println("Total NUmber of Edges : "+graph.numEdges)

//  Show In-degrees
  graph.inDegrees.sortBy(row=>row._2,false).take(10).foreach(println)

//  Show Out-degrees
  graph.outDegrees.sortBy(row=>row._2,false).take(10).foreach(println)

//  Trips From Station to Station
  graph
    .groupEdges((edge1, edge2) => edge1 + edge2)
    .triplets
    .sortBy(_.attr, ascending=false)
    .map(triplet =>
      "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
    .take(10)
    .foreach(println)

//  Station Rank
  val ranks = graph.pageRank(0.0001).vertices
  ranks.take(10).foreach(println)

//  Join Station Names and Sort By Rank
  ranks
    .join(vertices)
    .sortBy(_._2._1, ascending=false)
    .take(10)
    .foreach(println)



}
