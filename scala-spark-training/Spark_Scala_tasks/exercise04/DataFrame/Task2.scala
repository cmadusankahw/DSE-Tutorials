package Spark_Scala_tasks.exercise04.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task2 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("ClickStreamDataFrames").master("local[2]").getOrCreate()

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

  }
}
