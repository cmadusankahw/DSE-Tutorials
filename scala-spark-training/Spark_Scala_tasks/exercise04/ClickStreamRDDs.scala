package Spark_Scala_tasks.exercise04

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ClickStreamRDDs {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("ClickStreamRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val clickStream: RDD[String] = sc.textFile("in/clickstream.csv")

    // Task 2.1: No of Clicks Per User
    val ClicksPerUser = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(0)))
    val ClicksPerUserCount = ClicksPerUser.countByValue()

    // Task 2.1: Using a pair RDD
    val ClicksPerUserPairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(0), 1)).groupByKey()

    println("Task 2.1 : Clicks per User: " )
    for ((word, count) <- ClicksPerUserCount) println(word + " : " + count) // print word counts with a for loop
    ClicksPerUserPairRDD.saveAsTextFile("out/exercise03/Task_2_1_Clicks_Per_User.text")


    // Task 2.2: No of Clicks Per Product
    val ClicksPerProduct = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(2)))
    val ClicksPerProductCount = ClicksPerProduct.countByValue()

    // Task 2.2: Using a pair RDD
    val ClicksPerProductPairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(2), 1)).groupByKey()

    println("Task 2.2 : Clicks per Product: " )
    for ((word, count) <- ClicksPerProductCount) println(word + " : " + count) // print clicks per product with a for loop
    ClicksPerProductPairRDD.saveAsTextFile("out/exercise03/Task_2_2_Clicks_Per_Product.text")


    // Task 2.3: No of Clicks Per Product Category
    val ClicksPerProductCategory = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(1)))
    val ClicksPerProductCategoryCount = ClicksPerProductCategory.countByValue()

    // Task 2.3: Using a pair RDD
    val ClicksPerProductCategoryPairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(1), 1)).groupByKey()

    println("Task 2.3 : Clicks per Product Category: " )
    for ((word, count) <- ClicksPerProductCategoryCount) println(word + " : " + count) // print clicks per product category with a for loop
    ClicksPerProductCategoryPairRDD.saveAsTextFile("out/exercise03/Task_2_3_Clicks_Per_Product_Category.text")


    // Task 2.4: No of Clicks Per Channel
    val ClicksPerChannel = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(3)))
    val ClicksPerChannelCount = ClicksPerChannel.countByValue()

    // Task 2.4: Using a pair RDD
    val ClicksPerPChannelpairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(3), 1)).groupByKey()

    println("Task 2.4 : Clicks per Channel: " )
    for ((word, count) <- ClicksPerChannelCount) println(word + " : " + count) // print clicks per channel with a for loop
    ClicksPerPChannelpairRDD.saveAsTextFile("out/exercise03/Task_2_4_Clicks_Per_Channel.text")


    // Task 2.5: No of Clicks Per Product and Channel
    // Task 2.5: Using a pair RDD
    val ClicksPerProductChannelPairRDD = clickStream.map(line => ( ( line.split(Utils.COMMA_DELIMITER)(2), line.split(Utils.COMMA_DELIMITER)(3) ), 1))
      .groupByKey()

    ClicksPerProductChannelPairRDD.saveAsTextFile("out/exercise03/Task_2_5_Clicks_Per_Product_Channel.text")

    // Task 3: Top 5 Products
    // Using a pair RDD
    val TopProductsPairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(2), 1)).groupByKey().sortBy(_._2, false)

    TopProductsPairRDD.saveAsTextFile("out/exercise03/Task_3_Top_5_Products.text")


  }

}
