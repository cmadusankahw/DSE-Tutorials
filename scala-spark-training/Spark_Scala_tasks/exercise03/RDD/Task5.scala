package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Task5 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[1]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))

    // List the location and the price of houses
    val ListofLocationsandPrices: RDD[(String, Float)] = realEstateFinal.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(2).toFloat))


    // Task 5:  average price of houses to be sold in each city
    val LocatonsandAvaragePrices: RDD[(String, Float)] = ListofLocationsandPrices.mapValues(value => (value, 1)) // map entry with a count of 1
      .reduceByKey {
        case ((sumL, countL), (sumR, countR)) =>
          (sumL + sumR, countL + countR)
      }
      .mapValues {
        case (sum , count) => sum / count
      }

    val sortedLocatonsandAvaragePrices: RDD[(String, Float)] = LocatonsandAvaragePrices.sortByKey()

    // printing results
    println("Task 5: Average price of houses to be sold in each city: "+ sortedLocatonsandAvaragePrices)
    // saving result set
    sortedLocatonsandAvaragePrices.saveAsTextFile("out/Task_5_average_price_of_houses_in_each_city.text")

  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

}
