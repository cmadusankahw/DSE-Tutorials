package Spark_Scala_tasks.exercise04.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task3 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("ClickStreamDataFrames").master("local[2]").getOrCreate()
    import session.implicits._

    val clickStream = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/clickstream.csv").toDF("User","Product Category","Product", "Channel")


    // Task 3: Top 5 products according to the number of clicks
    val productList = clickStream
      .groupBy("Product")
      .count()
      .as("No of Clicks")

    productList.orderBy($"count".desc)
      .limit(5)
      .show(true)
  }
}
