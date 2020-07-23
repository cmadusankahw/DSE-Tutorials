import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Task3 extends App{

  val sc = new SparkContext("local[*]","Task3")
  val sqlContext = new SQLContext(sc)

  val schema = List(
    StructField("UserId",StringType, true),
    StructField("ProdCat",StringType, true),
    StructField("ProductId",StringType, true),
    StructField("Channel",StringType, true)
  )

  var df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").schema(StructType(schema)).load("/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv")

  def getCountByGivenMethodDF(df:DataFrame,colName:String):DataFrame={

    var count= df.groupBy(colName).count()
    return count
  }

  def getCountByMultipleMethodsDf(df:DataFrame,colName1:String,colName2:String):DataFrame={
    var count= df.groupBy(colName1,colName2).count()
    return count
  }

  def generateTopFiveProducts(df:DataFrame):DataFrame={
    var count = df.groupBy("ProductId").count().select("ProductId","count").orderBy(col("count").desc).limit(5)
    return count
  }

}
