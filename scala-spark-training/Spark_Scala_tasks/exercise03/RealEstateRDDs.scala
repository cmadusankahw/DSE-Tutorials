package Spark_Scala_tasks.exercise03

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

object RealEstateRDDs {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("realEstateRdd").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val realEstate: RDD[String] = sc.textFile("in/RealEstate.csv")

    // removing Header
    val realEstateFinal: RDD[String] = realEstate.filter(line => isNotHeader(line)).map(line => line.replace(" ", ""))

    // Task 1: filter and count Number of houses located in Santa Maria-Orcutt
    val housesInSantaMariaOrcutt: RDD[String] = realEstateFinal.filter(line => line.split(Utils.COMMA_DELIMITER)(1) == "Santa Maria-Orcutt")
    val housesInSantaMariaOrcuttCount: Long = housesInSantaMariaOrcutt.count()

    // Task 2: List the location and the price of houses which are priced over 500000
    val ListofLocationsandPrices: RDD[(String, Float)] = realEstateFinal.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(2).toFloat))

    val ListofLocationsandPrices500000: RDD[(String, Float)] = ListofLocationsandPrices.filter(keyValue => keyValue._2 > 500000)


    // Task 3: List the houses which have 3 bedrooms and available for short sale
    val Houseswith3BedroomsforShortSale: RDD[String] = realEstateFinal.filter(line => (line.split(Utils.COMMA_DELIMITER)(3).toInt == 3)
      && (line.split(Utils.COMMA_DELIMITER)(7) =="Short Sale"))


    // Task 4: Find the highest priced house in Cayucos which has more than 3 bedrooms and 2 bathrooms
    val highestPricedHouseinCayucos: RDD[String] = realEstateFinal.filter(line => (line.split(Utils.COMMA_DELIMITER)(1) =="Cayucos")
      && (line.split(Utils.COMMA_DELIMITER)(3).toInt > 3)
      && (line.split(Utils.COMMA_DELIMITER)(4).toInt > 2))

    val CayucosHousePrices: RDD[String] = highestPricedHouseinCayucos.map(line => line.split(Utils.COMMA_DELIMITER)(2))

    val CayucosHighetsHousePrice: Int = CayucosHousePrices.filter(number => !number.isEmpty)
      .map(number => number.toInt).reduce((x, y) => x + y)


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


    // Task 6:  Apply below aprivAvg equation and get the avg prices in a new column
    // avgPrice = (price+PriceSQFt*Size)/2
    val RealStattewithAvgColumn: RDD[Row] = realEstateFinal.map(row => {
      val originalColumns = row.toSeq.toList
      val newColumnValue = calcAvgPrice(originalColumns(2).toDouble,originalColumns(6).toDouble,originalColumns(5).toInt)
      Row.fromSeq(originalColumns :+ newColumnValue)})

    println("\nTask 2: Location and the price of houses which are priced over 500000: "+ ListofLocationsandPrices500000 )
    ListofLocationsandPrices500000.saveAsTextFile("out/Task_2_locations_and_house_prices_greater_than_500000.text")

    println("\nTask 3: Houses which have 3 bedrooms and available for short sale: "+ Houseswith3BedroomsforShortSale)
    Houseswith3BedroomsforShortSale.saveAsTextFile("out/Task_3_houses_with_3_bedrooms_for_short_sale.text")

    println("\nTask 4: Highest priced house in Cayucos with more than 3 bedrooms and 2 bathrooms: "+ CayucosHighetsHousePrice)
    highestPricedHouseinCayucos.saveAsTextFile("out/Task_4_house_prices_in_Cayucos.text")

    println("\nTask 5: Average price of houses to be sold in each city: "+ sortedLocatonsandAvaragePrices)
    sortedLocatonsandAvaragePrices.saveAsTextFile("out/Task_4_average_price_of_houses_in_each_city.text")

    println("\nTask 6:  aprivAvg equation applied as a new column: "+ RealStattewithAvgColumn)
    RealStattewithAvgColumn.saveAsTextFile("out/Task_6_real_estate_list_with_avg_column.text")
  }

  // Function to filter Header
  def isNotHeader(line: String): Boolean = !line.startsWith("MLS")

  // Function to calculate House Price Averages
  def calcAvgPrice(HousePrice: Double, SQFeetPrice: Double, Size: Integer): Double ={
    ( HousePrice + Size * SQFeetPrice ) / 2
  }

}

