package Spark_test.funSuite

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

object SimpleUnitTest extends FunSuite {

  // FunSuite: A Testing Style provided with sparkTest, scalaTest packages
  // In a unit test everything should be duplicated. (mocks should be defined instead of real data

  var expectedResult: List[(String, Int)] = List(("a", 3), ("b", 2), ("c", 4))

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  // your test case starts with "test" keyword
  test("Word counts should be equal to expected") {
    // function to be tested
    verifyWordCount(Seq("c a a b a c b c c"))
  }

  // duplicated/mocked function
  def verifyWordCount(seq: Seq[String]): Unit = {
    assertResult(expectedResult)() // function to test)
  }

}
