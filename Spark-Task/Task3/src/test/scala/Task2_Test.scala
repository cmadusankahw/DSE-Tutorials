import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, FunSuite}

class Task2_Test extends  FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{

  var path = "/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv"
  var sc:SparkContext=_
  var rddFromFile:RDD[String]=_

  override def beforeEach(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local[*]","Task2")
    rddFromFile = sc.textFile(path)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    sc.stop()
  }

 "Wen UserId is 1 clicks Count" should "be 1" in {
    assert(6===Task2.getCountByGivenSimpleMethodRdd(rddFromFile,0)(2)._2)
  }

  "When both product2 and webstore gives clicks" should "be 3" in {
    assert(3===Task2.getCountByGivenMultipleMethodsRdd(rddFromFile,2,3)(1)._2)
  }

  "When check for top 5 products" should "return product2,3,4,5,1" in{
    assert("product2"===Task2.getTopFiveProducts(rddFromFile)(0)._1)
    assert("product3"===Task2.getTopFiveProducts(rddFromFile)(1)._1)
    assert("product4"===Task2.getTopFiveProducts(rddFromFile)(2)._1)
    assert("product5"===Task2.getTopFiveProducts(rddFromFile)(3)._1)
    assert("product1"===Task2.getTopFiveProducts(rddFromFile)(4)._1)
  }

}
