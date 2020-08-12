package org.example.dateGeneration

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.mllib.stat._


object DateGeneration extends App {

  val sc=new SparkContext("local[*]","Random Date Generation")
  val spark = SparkSession.builder().appName("Random Date Generation").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  def generateCurrentDate(): LocalDateTime ={
    val offset = Timestamp.valueOf("2000-01-01 00:00:00").getTime
    val end = Timestamp.valueOf("2020-08-12 00:00:00").getTime
    val diff = end - offset + 1
    val rad = new Timestamp(offset + (Math.random()* diff).toLong)
    val date = rad.toLocalDateTime
    return date
  }

  def generateTomorrow(currentDate:LocalDateTime):LocalDateTime={
    var tomorrow = currentDate.plusDays(1)
    return tomorrow
  }

  def generateWeekTime(currentDate:LocalDateTime):LocalDateTime={
    var weekTime = currentDate.plusWeeks(1)
    return weekTime
  }

  def generateMonthTime(currentDate:LocalDateTime):LocalDateTime={
    var monthTime =currentDate.plusMonths(1)
    return monthTime
  }

  val formatter = DateTimeFormatter.ISO_DATE_TIME
  var currentDate:LocalDateTime =null

  var df = (1 to 100)
    .map(id => (id.toLong,

      {currentDate = generateCurrentDate()
      currentDate.format(formatter)},

      {var tomorrow = generateTomorrow(currentDate)
      tomorrow.format(formatter)},

      {var weekTime = generateWeekTime(currentDate)
        weekTime.format(formatter)},

      {var monthTime = generateMonthTime(currentDate)
      monthTime.format(formatter)}))

      .toDF("Id","Current-Time","Tomorrow-Time","Week-Time","Month-Time")

  df.show(10)

  /////////////////////////////////////////// Poisson Distribution //////////////////////////////////

  def getPoissonHours(lambda: Long) = {
    //    Lambda  = Rate Parameter
    //    https://en.wikipedia.org/wiki/Poisson_distribution#Generating_Poisson-distributed_random_variables
    val L = Math.exp(-lambda)
    var p = 1.0
    var k = 0

    do {
      k += 1
      p *= Math.random
    } while ( {
      p > L
    })
    k - 1
  }


 def generatedWithPoissonHours(): LocalDateTime ={
   val offset = Timestamp.valueOf("2000-01-01 00:00:00").getTime
   val end = Timestamp.valueOf("2020-08-12 00:00:00").getTime
   val diff = end - offset + 1
   val rad = new Timestamp(offset + (Math.random() * diff).toLong)
   val date = rad.toLocalDateTime
   var hour = -1
   while (hour<0||hour>24){
     hour = getPoissonHours(0.1.toLong)
   }
   date.withHour(hour)
   return date
 }

  var dfWithPoissonDistribution = (1 to 100)
    .map(id => (id.toLong,

      {currentDate = generateCurrentDate()
        currentDate.format(formatter)},

      {var tomorrow = generateTomorrow(currentDate)
        tomorrow.format(formatter)},

      {var weekTime = generateWeekTime(currentDate)
        weekTime.format(formatter)},

      {var monthTime = generateMonthTime(currentDate)
        monthTime.format(formatter)}))

    .toDF("Id","Current-Time","Tomorrow-Time","Week-Time","Month-Time")

  .show(10)



}
