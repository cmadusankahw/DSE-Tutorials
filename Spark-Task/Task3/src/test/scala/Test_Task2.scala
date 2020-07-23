import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, FunSuite}

class Test_Task2 extends  FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{


  var path = "/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv"

 "Wen UserId is 1 clicks Count" should "be 1" in {
    assert(6===Task2.getCountByGivenSimpleMethodRdd(path,0)(2)._2)
  }

  "When both product2 and webstore gives clicks" should "be 3" in {
    assert(3===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(2)._2)
  }

  "When check for top 5 products" should "return product2,3,4,5,1" in{
    assert("product2"===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(0)._2)
    assert("product3"===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(1)._2)
    assert("product4"===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(2)._2)
    assert("product5"===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(3)._2)
    assert("product1"===Task2.getCountByGivenMultipleMethodsRdd(path,2,3)(4)._2)
  }

}
