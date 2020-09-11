package Spark_Scala_tasks.exercise05

import faker._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object SyntheticDataGenerator {

  // user case class
  case class User(name: String, username: String, email: String, age: Integer)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // initializing sparkContext
    val conf: SparkConf = new SparkConf().setAppName("DataGenerator").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq.fill(100){randomUser()})

    rdd.saveAsTextFile("out/exercise06/generated_user_data.text")

  }

  // generate random age
  def randomAge(start: Integer, end: Integer): Integer = {
    start + Random.nextInt((end - start) + 1)
  }

  // generate random user
  def randomUser() : User = {
    User(Name.name, Internet.user_name(Name.name), Internet.free_email(Name.name), randomAge(18,25))
  }
}
