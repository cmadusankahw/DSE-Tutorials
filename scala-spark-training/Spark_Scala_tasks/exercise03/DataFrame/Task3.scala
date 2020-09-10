package Spark_Scala_tasks.exercise03.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Task3 {
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

    // Task 3: List the houses which have 3 bedrooms and available for short sale
    realEstate.filter(realEstate("Bedrooms") === 3 && realEstate("Status") === "Short Sale")
      .show(true)

  }
}
