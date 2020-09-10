package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))

    // Task 2: List the location and the price of houses which are priced over 500000
    val ListofLocationsandPrices: RDD[(String, Float)] = realEstateFinal.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(2).toFloat))

    val ListofLocationsandPrices500000: RDD[(String, Float)] = ListofLocationsandPrices.filter(keyValue => keyValue._2 > 500000)

    val sortedLocatonsandPrices: RDD[(String, Float)] = ListofLocationsandPrices500000.sortByKey()

    // printing results
    println("Task 2: Location and the price of houses which are priced over 500000: "+ sortedLocatonsandPrices )
    // saving result set
    sortedLocatonsandPrices.saveAsTextFile("out/Task_2_locations_and_house_prices_greater_than_500000.text")

  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

}
