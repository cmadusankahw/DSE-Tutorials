package Spark_Scala_tasks.exercise03.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task4 {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("RealStateDataFrames").master("local[2]").getOrCreate()
    import session.implicits._

    val realEstate = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/RealEstate.csv")

    // pre-processing data to remove unnecessary white-spaces
    realEstate.map(line => line.getString(1).replace(" ", ""))

    // Task 4: Find the highest priced house in Cayucos which has more than 2 bedrooms and 2 bathrooms
    realEstate.filter(realEstate("Location") === "Cayucos" && realEstate("Bedrooms") > 2 && realEstate("Bathrooms") > 2)
      .groupBy("Location")
      .max("Price")
      .as("Max House Price")
      .show(true)

  }
}
