package Spark_Scala_tasks.exercise03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RealEstateDataFrames {

  val PRICE_SQ_FT = "Price SQ Ft"

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

    // Task 1: Number of houses located in Santa Maria-Orcutt
    realEstate.filter(realEstate("Location") === "Santa Maria-Orcutt")
      .groupBy("Location")
      .count()
      .as("No of Houses")
      .show(true)

    // Task 2: location and the price of houses which are priced over 500000
    realEstate.filter(realEstate("Price") > 500000)
      .select("Location", "Price")
      .show(true)

    // Task 3: List the houses which have 3 bedrooms and available for short sale
    realEstate.filter(realEstate("Bedrooms") === 3 && realEstate("Status") === "Short Sale")
      .show(true)

    // Task 4: Find the highest priced house in Cayucos which has more than 2 bedrooms and 2 bathrooms
    realEstate.filter(realEstate("Location") === "Cayucos" && realEstate("Bedrooms") > 2 && realEstate("Bathrooms") > 2)
      .groupBy("Location")
      .max("Price")
      .as("Max House Price")
      .show(true)

    // Task 5:  average price of houses to be sold in each city
    realEstate
      .groupBy("Location")
      .avg("Price")
      .as("Average Price of Houses")
      .show(true)


    // Task 6:  Apply below aprivAvg equation and get the avg prices in a new column
    // avgPrice = (price+PriceSQFt*Size)/2
    val avgFunc = udf((HousePrice: Double, SQFeetPrice: Double, Size: Integer) => {( HousePrice + Size * SQFeetPrice ) / 2})
    realEstate.withColumn("Average Price"
      , avgFunc(realEstate("Price"),realEstate("Price SQ Ft"),realEstate("Size"))).show(true)


    // realState data grouped by location
    realEstate.groupBy("Location")
      .avg(PRICE_SQ_FT)
      .orderBy("avg(Price SQ Ft)")
      .show()
  }

}
