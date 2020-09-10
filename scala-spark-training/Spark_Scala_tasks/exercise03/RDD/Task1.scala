package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line))

    // filter and count Number of houses located in Santa Maria-Orcutt
    val housesInSantaMariaOrcutt: RDD[String] = realEstateFinal.filter(line => line.split(Utils.COMMA_DELIMITER)(1) == "Santa Maria-Orcutt")
    val housesInSantaMariaOrcuttCount: Long = housesInSantaMariaOrcutt.count()

    // printing results
    println("Task 1: Number of houses located in Santa Maria-Orcutt: " + housesInSantaMariaOrcuttCount)

  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

}
