import Task3.sqlContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}

class Task3_Test extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach{

  var sc:SparkContext=_
  var sqlContext:SQLContext=_

  var df:DataFrame=_
  override def beforeEach(): Unit = {
    super.beforeAll()
    val sparkConf = new SparkConf()
      .setAppName("Task3")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
    sqlContext=new SQLContext(sc)

    val schema = List(
      StructField("UserId",StringType, true),
      StructField("ProdCat",StringType, true),
      StructField("ProductId",StringType, true),
      StructField("Channel",StringType, true)
    )
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").schema(StructType(schema)).load("/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    sc.stop()
  }

  "When Count Clicks for Product 3,5,1 "should "Return 3,2,1 as count "in{

    val expectedData=Seq(
      Row("product3",3),
      Row("product5",2),
      Row("product1",2)
    )

    val expectedSchema = List(
      StructField("ProdId",StringType, true),
      StructField("count",IntegerType, true)
    )
    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(expectedDF.collect().toSeq===Task3.getCountByGivenMethodDF(df,"ProductId").take(3).toSeq)

  }

  "When count clicks for both product and webStore it "should "Return 3 "in{
    val expectedData=Seq(
      Row("product8","Tablet",1),
      Row("product6","Webstore",1),
      Row("product1","Webstore",2)
    )

    val expectedSchema = List(
      StructField("ProdId",StringType, true),
      StructField("channel",StringType, true),
      StructField("count",IntegerType, true)
    )
    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(expectedDF.collect().toSeq===Task3.getCountByMultipleMethodsDf(df,"ProductId","Channel").take(3).toSeq)
  }


}
