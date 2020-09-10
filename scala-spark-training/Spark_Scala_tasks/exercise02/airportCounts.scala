package Spark_Scala_tasks.exercise02

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object airportCounts {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf = new SparkConf().setAppName("airportCounts").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read airports.text file as a RDD
    val airports = sc.textFile("in/airports.text")

    // filter Details of Airports in Ireland
    val airportsInIreland = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"Ireland\"")

    // filter Details of Airports in USA
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    // get the Airports in Ireland count
    val airportsInIrelandCount = airportsInIreland.count()

    // list airports, whose latitude are greater than 40
    val airportsLatitudeGreaterThan40 = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    // create RDD with airport names and latitudes of all airports
    val airportNamesAndLatitudes = airports.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    // create RDD with airport names and city names of airports in USA
    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })

    // create RDD with country and List of names of airports
    val countryAndAirportNameAndPair = airports.map(airport => (airport.split(Utils.COMMA_DELIMITER)(3),
      airport.split(Utils.COMMA_DELIMITER)(1)))
    // group airports by key(country name)
    val airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    // printing results
    println("1 countOfAirportsInIreLand: " + airportsInIrelandCount)
    println("\n2\na) countLatitudeGreaterThan40: "+ airportsLatitudeGreaterThan40 )

    // printing airports grouped by country names list
    println("\n4 List of Airports by Country Names: ")
    for ((country, airportName) <- airportsByCountry.collectAsMap()) println(country + ": " + airportName.toList)

    // store result set in to files
    airportNamesAndLatitudes.saveAsTextFile("out/airports_by_latitude.text")
    println("2\nb) Airport Names and Latitudes Fle Saved")
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
    println("3 Airport Names and City Names of Airports in USA file Saved")
  }
}

// comma delimiter initializing
object Utils {
  // a regular expression which matches commas but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
}
