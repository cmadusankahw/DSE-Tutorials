
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.functions._


object Task2 extends App {

  val sc=new SparkContext("local[*]","Task2")
  val spark = SparkSession.builder().appName("Task2").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  val schema = new StructType()
    .add("id",IntegerType,true)
    .add("firstName",IntegerType,true)

  val rddFirstNamesFromFile = spark.sparkContext.textFile("/home/yasasm/Desktop/ZoneProjects/spark-tasks/src/main/inputs/firstNames")

  var rddFirstNames =rddFirstNamesFromFile.flatMap(lines=>lines.split(","))

  var dfFirstNames= rddFirstNames.toDF("values")

  dfFirstNames.show()

  var FirstNames= dfFirstNames.select(split(col("values")," ").getItem(0).as("FirstName"),
    split(col("values")," ").getItem(1).as("id")
  )

  FirstNames= FirstNames.withColumn("id",col("id").cast(IntegerType))
  FirstNames.printSchema()
  FirstNames.show(false)

  val rddLastNamesFromFile= spark.sparkContext.textFile("/home/yasasm/Desktop/ZoneProjects/spark-tasks/src/main/inputs/lastNames")

  var dfLastNames = rddLastNamesFromFile.toDF("values")

  dfLastNames.show()

  var LastNames= dfLastNames.select(split(col("values")," ").getItem(0).as("id"),
    split(col("values")," ").getItem(1).as("LastName")
  ).drop("values")

  LastNames = LastNames.withColumn("id",col("id").cast(IntegerType))


  LastNames.printSchema()
  LastNames.show()

  var names= LastNames.join(FirstNames,Seq("id")).where(LastNames("id")===FirstNames("id"))

  names.show()

  names.rdd.map(x => x.mkString("|")).saveAsTextFile("names.txt")

//  val output= names.rdd.map(_.toString()).saveAsTextFile("/home/yasasm/Desktop/ZoneProjects/names.txt")
















}
