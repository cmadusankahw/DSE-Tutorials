package org.example.randomRDD

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs.poissonRDD
import org.apache.spark.sql.SparkSession
import org.example.randomRDD.Tutorial.sc

object DateGenerationUsingRandomRdd extends App {

  val sc=new SparkContext("local[*]","Random Date Generation RDD")
  val spark = SparkSession.builder().appName("Random Data Generation RDD").getOrCreate()
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


  def getPoissonHours(lambda: Long): Int = {
    var hour = -1
    while (hour<0||hour>23) {
      val poisson_distributed_rdd = poissonRDD(sc, lambda, 1)
      hour = poisson_distributed_rdd.collect()(0).toInt
    }
    return hour
  }


  ////////////////// Check Poisson Distribution of Hours  //////////////////

  var defaultDay:LocalDateTime = null
  var weekTime:LocalDateTime = null
  var monthTime:LocalDateTime=null

  val formatter = DateTimeFormatter.ISO_DATE_TIME

  var sameDayHourlyDistribution = (1 to 100)
    .map(id => (id.toLong,
      {
        defaultDay = generateCurrentDateToTomorrow()
        var hour = getPoissonHours(10.2.toLong)
        defaultDay=defaultDay.withHour(hour)
        defaultDay.format(formatter)
      },

      {
        weekTime = generateCurrentDateToWeekTime()
        var hour = getPoissonHours(10.2.toLong)
        weekTime = weekTime.withHour(hour)
        weekTime.format(formatter)},

      {
        monthTime = generateCurrentDateToMonthTime()
        var hour = getPoissonHours(10.2.toLong)
        monthTime = monthTime.withHour(hour)
        monthTime.format(formatter)
      }
    )
    )
    .toDF("Id","Current-Tomorrow","Current-Week","Current-Month")

  sameDayHourlyDistribution.show(20)
}
