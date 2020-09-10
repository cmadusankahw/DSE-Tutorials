package Spark_test.flatSpec

import org.apache.spark.sql.SparkSession
import org.scalatest._

class ClickStreamDataTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private val master = "local[*]"
  private val appName = "clickStreamTest"

  var spark: SparkSession = _

  override def beforeEach() {
    spark = new SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  "Creating dataframe" should "produce data from of correct size" in {
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