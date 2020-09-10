package Spark_Scala_tasks.exercise03.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Task6 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))

    // Task 6:  Apply below aprivAvg equation and get the avg prices in a new column
    // avgPrice = (price+PriceSQFt*Size)/2
    val RealStattewithAvgColumn = realEstateFinal.map(row =>
      row ++ "," +calcAvgPrice(row.split(Utils.COMMA_DELIMITER)(2).toDouble,row.split(Utils.COMMA_DELIMITER)(6).toDouble, row.split(Utils.COMMA_DELIMITER)(5).toInt ).toString )

    // printing results
    println("Task 6:  aprivAvg equation applied as a new column: "+ RealStattewithAvgColumn)
    RealStattewithAvgColumn.saveAsTextFile("out/Task_6_real_estate_list_with_avg_column.text")
  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

  // Function to calculate House Price Averages
  def calcAvgPrice(HousePrice: Double, SQFeetPrice: Double, Size: Integer): Double ={
    ( HousePrice + Size * SQFeetPrice ) / 2
  }
}
