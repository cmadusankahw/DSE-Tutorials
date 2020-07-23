import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, FunSuite}

import scala.collection.immutable.HashMap

class Test_Task1 extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{


  "When Count clicks for user 1 it "should "Return 6 "in {
    assert(6 === Task1.countClicksPerGivenSingleMethod(0).get("user1"))

  }

  "When Count Clicks for both product and webStore it "should "Return 3 "in  {

    var items = List("product2","Webstore")
    assert(3===Task1.countClicksPerGivenAnyMultipleMethods(2,3).get(items))

  }



}


