
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.functions._
import java.io
import java.io.{File, PrintWriter}


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

  var firstNames= dfFirstNames.select(split(col("values")," ").getItem(0).as("FirstName"),
    split(col("values")," ").getItem(1).as("id")
  )

  firstNames= firstNames.withColumn("id",col("id").cast(IntegerType))
  firstNames.printSchema()
  firstNames.show(false)

  val rddLastNamesFromFile= spark.sparkContext.textFile("/home/yasasm/Desktop/ZoneProjects/spark-tasks/src/main/inputs/lastNames")

  var dfLastNames = rddLastNamesFromFile.toDF("values")

  dfLastNames.show()

  var lastNames= dfLastNames.select(split(col("values")," ").getItem(0).as("id"),
    split(col("values")," ").getItem(1).as("LastName")
  ).drop("values")

  lastNames = lastNames.withColumn("id",col("id").cast(IntegerType))

  lastNames.printSchema()
  lastNames.show()

  var names= lastNames.join(firstNames,Seq("id")).where(lastNames("id")===firstNames("id"))

  names.show()

//  names.rdd.map(x => x.mkString("|")).saveAsTextFile("names.txt")

//  val output= names.rdd.map(_.toString()).saveAsTextFile("/home/yasasm/Desktop/ZoneProjects/names.txt")


//////////////////////////////////  Using SQL Statements ///////////////////////////////////

  var firstNameTable = firstNames.registerTempTable("firstNameTable")
  var lastNameTable = lastNames.registerTempTable("lastNameTable")

  var fullNames = sqlContext.sql("SELECT f.id,f.FirstName,l.LastName FROM firstNameTable as f INNER JOIN lastNameTable as l on l.id = f.id");
  fullNames.collect().foreach(println)


  val pw = new PrintWriter(new File("fullnames.txt"))

  fullNames.collect().foreach(
    row=>(row.toSeq.foreach(col=>pw.write(s"$col ")),pw.write("\n"))
  )

  }


