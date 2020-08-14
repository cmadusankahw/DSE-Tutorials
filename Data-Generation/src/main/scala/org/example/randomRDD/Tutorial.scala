package org.example.randomRDD

import java.time.LocalDateTime

import org.apache.commons.math3.random.{RandomDataGenerator, RandomGenerator}
import org.apache.spark.mllib.random.PoissonGenerator
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.example.RandomDateGeneration

import scala.util.Random


object Tutorial extends App {

//  Rdd With Normal Distribution
  val sc=new SparkContext("local[*]","Random Data Generation")
  val spark = SparkSession.builder().appName("Random Data Generation").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val normal_distributed_rdd =normalRDD(sc,100,10,1.0.toLong)
  normal_distributed_rdd.toDF("Values").show(10)


//  Generate Rdd With Given Mean and Std with Normal Distribution
  val mean = 400.0
  val std = 899.73
  val customized_normal_distributed_rdd =normalRDD(sc,100,10,1.0.toLong).map(x=>mean+std*x)
  customized_normal_distributed_rdd.toDF("Values").show(10)
  var stats = customized_normal_distributed_rdd.stats()
  println("Mean Is : "+stats.mean)
  println("STDEV Is : "+stats.stdev)
  println("Variance Is : "+stats.variance)


//  Compute Kernel Density of a RDD
  val kd = new KernelDensity()
    .setSample(customized_normal_distributed_rdd)
    .setBandwidth(3.0)

  val densities = kd.estimate(Array(-1.0, 2.0, 5.0))

  densities.foreach(println)
  

//  Normal Vector RDD
  val normal_vector_rdd = normalVectorRDD(sc,100,2)
  normal_vector_rdd.take(10).foreach(println)


//  Generate Poisson Distributed Values
  val poisson_distributed_rdd = poissonRDD(sc,10.2,100,10)
  poisson_distributed_rdd.toDF("Poisson_values").show(10)
  stats = poisson_distributed_rdd.stats()

  println("Mean Is : "+stats.mean)
  println("STDEV Is : "+stats.stdev)


//  Poisson Vector Rdd
  val poisson_vector_rdd = poissonVectorRDD(sc,10.2,100,3)
  poisson_vector_rdd.take(10).foreach(println)

// Uniform Distribution (Equally Like Unique OutComes [A Fair Die])
  val uniform_distributed_rdd = uniformRDD(sc,100,10,123456).map(x=>x%6)
  uniform_distributed_rdd.take(10).foreach(println)

  println(uniform_distributed_rdd.max()+","+uniform_distributed_rdd.min())

//  Generate random Data For Die
  def getRandomNumbers(min: Int, max: Int): Int = {
    return Random.nextInt(max - min) + min
  }

//  For Two People - Die Game
  var die_two_people_df = (1 to 100)
    .map(id => (id.toLong,
      getRandomNumbers(1,7),
      getRandomNumbers(1,7)))
    .toDF("Event","Outputs-Person 1","Outputs-Person 2")

  die_two_people_df.show(10)

//  Get Count that both users get same number
  var count = die_two_people_df.filter(die_two_people_df("Outputs-Person 1")===die_two_people_df("Outputs-Person 2")).count()
  println(s"Count that both users get same value  : $count")

//  Uniform Vector Rdd
  val uniform_vector_rdd = uniformVectorRDD(sc,100,2)
  uniform_vector_rdd.take(10).foreach(println)


//  Poison Distribution - Method 3
  val random_rdd = randomRDD(sc,new PoissonGenerator(12.0),100,10)
  random_rdd.take(10).foreach(println)



}
