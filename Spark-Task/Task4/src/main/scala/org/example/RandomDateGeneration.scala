package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, rand, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS

import scala.collection.immutable.HashMap
import scala.util.Random

object RandomDateGeneration extends App{

  val sc=new SparkContext("local[*]","Random Date Generation")
  val spark = SparkSession.builder().appName("Random Date Generation").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def random(size:Int): HashMap[String,String] = {

    var dates=new HashMap[String,String]

    for(x <- 0 to size+1) {
      val from = LocalDate.of(1999, 1, 1)
      val toRecruited = LocalDate.of(2019, 7, 28)
      val toRetired = LocalDate.now()

      val random = new Random(System.nanoTime)
      var diff = DAYS.between(from, toRecruited)
      var recruitedDate = from.plusDays(random.nextInt(diff.toInt))

      diff =DAYS.between(recruitedDate,toRetired)

      var retiredDate = from.plusDays(random.nextInt(diff.toInt))

      if(retiredDate.isBefore(recruitedDate)){
        var temp=retiredDate
        retiredDate=recruitedDate
        recruitedDate=temp
      }

      import java.time.format.DateTimeFormatter
      val formatter = DateTimeFormatter.ofPattern("dd MMMM yyyy")
      val formattedRecruited =recruitedDate.format(formatter)
      val formattedRetired=retiredDate.format(formatter)

      dates = dates+(formattedRecruited->formattedRetired)

    }
  dates
  }

  var size:Int=100

  var df = (1 to size)
    .map(id => (id.toLong))
    .toDF("id")

  var firstDF = df.sample(false, 0.5)

  var secondDF = df.except(firstDF)

  firstDF.describe().show()
  secondDF.describe().show()


  var dates=random(size).toSeq

  var df1 = (1 to size)
    .map(id => (id.toLong,dates(id-1)._1,dates(id-1)._2))
    .toDF("dateId","Recruited Date","Retired date")

  firstDF = firstDF.join(df1,firstDF("id")===df1("dateId"))
  firstDF.take(20).foreach(println)

  secondDF = secondDF.join(df1,secondDF("id")===df1("dateId"))
  secondDF.take(20).foreach(println)

}

