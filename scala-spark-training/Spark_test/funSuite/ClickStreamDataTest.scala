package Spark_test.funSuite

import org.apache.spark.sql.SparkSession
import org.scalatest._

class ClickStreamDataTest extends FunSuite with Matchers with BeforeAndAfterEach {

  private val master = "local[*]"
  private val appName = "clickStreamTest"

  var spark: SparkSession = _

  override def beforeEach() {
    spark = new SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  test("Creating dataframe should produce data from of correct size") {
    val sQLContext = spark.sqlContext

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/clickstream.csv").toDF("User","Product Category","Product", "Channel")

    // asserting tests
    assert(df.count() == 20)
    assert(df.take(1)(0).equals("user1"))
  }

  override def afterEach() {
    spark.stop()
  }

}