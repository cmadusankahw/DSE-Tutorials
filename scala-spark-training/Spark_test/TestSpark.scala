package Spark_test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite}

object TestSpark extends FunSuite {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = Seq(
      Seq("a", "1"),
      Seq("b", "2"),
      Seq("c", "3"),
      Seq("e", "4")
    ).map(x =>(x(0), x(1)))
      .toDF("key","value")

    df.show()

  }
}