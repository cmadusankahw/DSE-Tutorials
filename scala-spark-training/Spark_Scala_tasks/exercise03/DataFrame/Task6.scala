package Spark_Scala_tasks.exercise03.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Task6 {
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

    // Task 6:  Apply below aprivAvg equation and get the avg prices in a new column
    // avgPrice = (price+PriceSQFt*Size)/2
    val avgFunc = udf((HousePrice: Double, SQFeetPrice: Double, Size: Integer) => {( HousePrice + Size * SQFeetPrice ) / 2})
    realEstate.withColumn("Average Price"
      , avgFunc(realEstate("Price"),realEstate("Price SQ Ft"),realEstate("Size"))).show(true)

  }
}
