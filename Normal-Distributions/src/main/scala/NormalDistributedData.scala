package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.example.RandomDateGeneration.dates


import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

import scala.util.Random

object NormalDistributedData extends App {

  val sc = new SparkContext("local[*]", "Random Date Generation")
  val spark = SparkSession.builder().appName("Normal Distributed Data").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  var size: Int = 100

  var df = (1 to size)
    .map(id => (id.toLong))
    .toDF("id")

  df.show()

  def generateRandoms(size: Int,min:Int,max:Int): Array[Double] = {
    val list = new Array[Double](size)

    val mean = 400.0
    val std = 899.73
    for (i <- 0 until list.length) {

      //   Random.nextGaussian ->   https://www.javamex.com/tutorials/random_numbers/gaussian_distribution_2.shtml
      var num:Double = mean + std * Random.nextGaussian
      while(!(num>min && num<max)) {
        num = mean + std * Random.nextGaussian
      }
      list(i) = num
    }
    list
  }

  var numbers: Array[Double] = generateRandoms(100,200,600)

  var df1 = (1 to size)
    .map(id => (Random.nextInt(100), numbers(id - 1)))
    .toDF("id", "Number")

  df1.show()


  val plot = Vegas("Country Pop").
    withDataFrame(df1).
    encodeX("id", Nom).
    encodeY("Number", Quant).
    mark(Bar)

//  println(plot.toJson)
//  Paste the generated JSON in to https://vega.github.io/editor/#/edited

  val plot2 = Vegas("Plot").
    withDataFrame(df1).
    encodeX("Number",Quantitative).
    encodeY("Number",Quantitative).
    mark(Bar)

  println(plot2.toJson)
  
}


