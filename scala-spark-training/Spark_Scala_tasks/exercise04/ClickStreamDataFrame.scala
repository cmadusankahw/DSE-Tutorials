package Spark_Scala_tasks.exercise04

import Spark_Scala_tasks.exercise03.RealEstateDataFrames.PRICE_SQ_FT
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.control.Exception.noCatch.desc

object ClickStreamDataFrame {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("ClickStreamDataFrames").master("local[2]").getOrCreate()
    import session.implicits._

    val clickStream = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/clickstream.csv").toDF("User","Product Category","Product", "Channel")

    // Task 2.1 : No of Clicks Per User
    clickStream
      .groupBy("User")
      .count()
      .as("No of Clicks")
      .show(true)

    // Task 2.2 : No of Clicks Per Product
    clickStream
      .groupBy("Product")
      .count()
      .as("No of Clicks")
      .show(true)

    // Task 2.3 : No of Clicks Per Product Category
    clickStream
      .groupBy("Product Category")
      .count()
      .as("No of Clicks")
      .show(true)

    // Task 2.4 : No of Clicks Per Channel
    clickStream
      .groupBy("Channel")
      .count()
      .as("No of Clicks")
      .show(true)

    // Task 2.5 : No of Clicks Per Product and Channel
    clickStream
      .groupBy("Product", "Channel")
      .count()
      .as("No of Clicks")
      .show(true)

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
