package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))

    // Task 3: List the houses which have 3 bedrooms and available for short sale
    val Houseswith3BedroomsforShortSale: RDD[String] = realEstateFinal.filter(line => (line.split(Utils.COMMA_DELIMITER)(3).toInt == 3)
      && (line.split(Utils.COMMA_DELIMITER)(7) =="Short Sale"))

    // printing results
    println("Task 3: Houses which have 3 bedrooms and available for short sale: "+ Houseswith3BedroomsforShortSale)
    // saving result set
    Houseswith3BedroomsforShortSale.saveAsTextFile("out/Task_3_houses_with_3_bedrooms_for_short_sale.text")

  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

}
