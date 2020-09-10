package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task4 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))


    // Task 4: Find the highest priced house in Cayucos which has more than 2 bedrooms and 2 bathrooms
    val highestPricedHouseinCayucos: RDD[String] = realEstateFinal.filter(line => (line.split(Utils.COMMA_DELIMITER)(1) =="Cayucos")
      && (line.split(Utils.COMMA_DELIMITER)(3).toInt > 2)
      && (line.split(Utils.COMMA_DELIMITER)(4).toInt > 2))

    val CayucosHousePrices: RDD[String] = highestPricedHouseinCayucos.map(line => line.split(Utils.COMMA_DELIMITER)(2))

    val CayucosHighetsHousePrice: Float = CayucosHousePrices.map(number => number.toFloat).max()

    // printing results
    println("Task 4: Highest priced house in Cayucos with more than 3 bedrooms and 2 bathrooms: "+ CayucosHighetsHousePrice)
    // saving result set
    highestPricedHouseinCayucos.saveAsTextFile("out/Task_4_house_prices_in_Cayucos.text")

  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

}
