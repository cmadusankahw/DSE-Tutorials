package org.example.dateGeneration

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat._
import org.apache.spark.sql.expressions.Window
import org.example.NormalDistributedData.df1
import vegas.sparkExt.VegasSpark
import vegas.{Bar, Nom, Nominal, Point, Quant, Quantitative, Vegas}


object DateGeneration extends App {

  val sc=new SparkContext("local[*]","Random Date Generation")
  val spark = SparkSession.builder().appName("Random Date Generation").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  def generateCurrentDateToTomorrow(): LocalDateTime ={
    val today =Timestamp.valueOf(LocalDateTime.now()).getTime
    val tomorrow = Timestamp.valueOf(LocalDateTime.now().plusDays(1)).getTime
    val diff = tomorrow - today + 1
    val rad = new Timestamp(today + (Math.random()* diff).toLong)
    val date = rad.toLocalDateTime
    return date
  }


  def generateCurrentDateToWeekTime():LocalDateTime={
    val today =Timestamp.valueOf(LocalDateTime.now()).getTime
    val week = Timestamp.valueOf(LocalDateTime.now().plusWeeks(1)).getTime
    val diff = week - today + 1
    val rad = new Timestamp(today + (Math.random()* diff).toLong)
    val date = rad.toLocalDateTime
    return date
  }

  def generateCurrentDateToMonthTime():LocalDateTime={
    val today =Timestamp.valueOf(LocalDateTime.now()).getTime
    val month = Timestamp.valueOf(LocalDateTime.now().plusMonths(1)).getTime
    val diff = month - today + 1
    val rad = new Timestamp(today + (Math.random()* diff).toLong)
    val date = rad.toLocalDateTime
    return date
  }

  val formatter = DateTimeFormatter.ISO_DATE_TIME
  
  var df = (1 to 100)
    .map(id => (id.toLong,

      {var currentDate = generateCurrentDateToTomorrow()
      currentDate.format(formatter)},

      {var weekTime = generateCurrentDateToWeekTime()
        weekTime.format(formatter)},

      {var monthTime = generateCurrentDateToMonthTime()
      monthTime.format(formatter)}))

      .toDF("Id","Current-Tomorrow","Current-Week","Current-Month")

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

  ////////////////// Check Poisson Distribution of Hours  //////////////////

  var defaultDay:LocalDateTime = null
  var weekTime:LocalDateTime = null
  var monthTime:LocalDateTime=null

  var sameDayHourlyDistribution = (1 to 100)
    .map(id => (id.toLong,
      {
        defaultDay = generateCurrentDateToTomorrow()
        var hour = -1
        while (hour<0||hour>23){
          hour = getPoissonHours(10.2.toLong)
        }
        defaultDay=defaultDay.withHour(hour)
        defaultDay.format(formatter)
        },

      {
        weekTime = generateCurrentDateToWeekTime()
        var hour = -1
        while (hour<0||hour>23){
          hour = getPoissonHours(10.2.toLong)
        }
        weekTime = weekTime.withHour(hour)
        weekTime.format(formatter)},

      {
        monthTime = generateCurrentDateToMonthTime()
        var hour = -1
        while (hour<0||hour>23){
          hour = getPoissonHours(10.2.toLong)
        }
        monthTime = monthTime.withHour(hour)
        monthTime.format(formatter)
      }
      )
    )
    .toDF("Id","Current-Tomorrow","Current-Week","Current-Month")

   sameDayHourlyDistribution.show(20)


  var current_tomorrow = Vegas("current-tomorrow-plot")
    .withDataFrame(sameDayHourlyDistribution.limit(20))
    .encodeX("Id",Nominal)
    .encodeY("Current-Tomorrow")
    .mark(Point)

  println(current_tomorrow.toJson)


  var current_week = Vegas("current-week-plot")
    .withDataFrame(sameDayHourlyDistribution.limit(20))
    .encodeX("Id",Nominal)
    .encodeY("Current-Week")
    .mark(Point)

  println(current_week.toJson)


  var current_month = Vegas("current-month-plot")
    .withDataFrame(sameDayHourlyDistribution.limit(20))
    .encodeX("Id",Nominal)
    .encodeY("Current-Month")
    .mark(Point)

  println(current_month.toJson)

  //  Paste the generated JSON in to https://vega.github.io/editor/#/edited

}
