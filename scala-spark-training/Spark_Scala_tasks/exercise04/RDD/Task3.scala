package Spark_Scala_tasks.exercise04.RDD

import Spark_Scala_tasks.exercise02.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Task3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("ClickStreamRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read RealState.text file as a RDD
    val clickStream: RDD[String] = sc.textFile("in/clickstream.csv")

    // Task 3: Top 5 Products
    // Using a pair RDD
    val TopProductsPairRDD = clickStream.map(line => (line.split(Utils.COMMA_DELIMITER)(2), 1)).groupByKey().sortBy(_._2, false)

    TopProductsPairRDD.saveAsTextFile("out/exercise03/Task_3_Top_5_Products.text")

  }
}
