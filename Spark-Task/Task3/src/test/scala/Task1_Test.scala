import org.scalatest.{FlatSpec, Matchers}

class Task1_Test extends FlatSpec with Matchers {

  "com.starter" should "run a sample test" in {
    assert(21==2)
  }

  "com.starterljkl" should "run a sample test" in {
    assert(6===Task1.countClicksPerGivenSingleMethod(0).get("user1"))
  }

  "When Count Clicks for both product and webStore it "should "Return 3 "in  {

    var items = List("product2","Webstore")
    assert(3===Task1.countClicksPerGivenAnyMultipleMethods(2,3).get(items))

  }



}
