package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat._

import scala.util.Random

object Distributions extends App {

  val sc = new SparkContext("local[*]", "Random Date Generation")
  val spark = SparkSession.builder().appName("Normal Distributed Data").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

///////////////////////////////// Test ///////////////////////////////
  val sample = sc.parallelize(Seq(0.0,1.0,4.0,4.0))

  val kd= new KernelDensity()
    .setSample(sample)
    .setBandwidth(3)

  val sampleDensities = kd.estimate(Array(-1.0,2.0,5.0))

  sampleDensities.foreach(println)


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

  val u = normalRDD(sc,10000L,10)

  val size:Int=100

  var data = (1 to size)
    .map(id => numbers(id - 1))

  val rdd = sc.parallelize(data)

  val density = new KernelDensity()
    .setSample(rdd)
    .setBandwidth(5)

  val densities = density.estimate(Array(200,300,400,500,600))

  densities.foreach(println)


  ///////////////////////// Random Data Generation Using normalRddMethod ///////////////////////

  val rdd2 = normalRDD(sc,100L,10)

  val mappedWithStdAndMean = rdd2.map(x=>400+800.93*x)

  mappedWithStdAndMean.take(20).foreach(println)

  val density2 = new KernelDensity()
    .setSample(mappedWithStdAndMean)
    .setBandwidth(4)

  val densities2= density2.estimate(Array(-500,0,500,1000,1500))

  densities2.foreach(println)

}
