import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterAll, AfterEach, Assertions, BeforeAll, BeforeEach, Test}


class Task_Test {

  var sc:SparkContext=_
  var sqlContext:SQLContext=_

  var df:DataFrame=_

  @BeforeEach
  def before(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Spark Batch")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
    sqlContext=new SQLContext(sc)

    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/home/yasasm/Desktop/ZoneProjects/spark-Azkaban/src/main/inputs/athlete_events.csv")

    df = df.withColumn("Age", col("Age").cast(IntegerType))
      .withColumn("Height", col("Height").cast(IntegerType))
      .withColumn("Weight", col("Weight").cast(IntegerType))
      .withColumn("Year", col("Year").cast(IntegerType))
  }

  @AfterEach
  def after(): Unit = {
    sc.stop()
  }

  @Test
  def testGivesTotalNumberOfAthletes_when_GivesAValidDataFrame() = {

    val expectedData = Seq(
      Row(135571)
    )

    val expectedSchema = List(
      StructField("name",IntegerType, true)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getTotalNumberOfAthletes(df).collect().toSeq)

  }

  @Test
  def testGoldMedalWinners_when_GivesAValidDataFrame()={

    val expectedData=Seq(
      Row("100000","George Haddow Rennie","Gold"),
      Row("100050","Attila Repka","Gold")
    )

    val expectedSchema = List(
      StructField("ID",StringType, true),
      StructField("Name",StringType, true),
      StructField("Medal",StringType, true)
    )
    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getGoldMedalWinners(df).take(2).toSeq)

  }

  @Test
  def testCountryWithMaxGoldMedalsWithYear_when_GiveAValidDataFrame()={

    val expectedData=Seq(
      Row("Soviet Union",1980,201)
    )

    val expectedSchema = List(
      StructField("Team",StringType, true),
      StructField("Year",IntegerType, true),
      StructField("count",IntegerType, true)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getCountryWithMAxGoldMedals(df).collect().toSeq)
  }

   @Test
  def testAverageHeightOfMen_when_GiveAValidDataFrame()={

     val expectedData=Seq(
       Row("M",178.84931525982415)
     )

     val expectedSchema = List(
       StructField("Sex",StringType, true),
       StructField("Average Height of Men",DoubleType, true)
     )

     val expectedDF = sqlContext.createDataFrame(
       sqlContext.sparkContext.parallelize(expectedData),
       StructType(expectedSchema)
     )

     Assertions.assertEquals(expectedDF.collect().toSeq,Task.getAverageHeightOfMen(df).collect().toSeq)

   }

  @Test
  def testAverageWeightOfWomen_when_GiveAValidDataFrame()={

    val expectedData=Seq(
      Row("F",60.02838454152664)
    )

    val expectedSchema = List(
      StructField("Sex",StringType, true),
      StructField("Average weight of Women",DoubleType, true)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getAverageWeightOfWomen(df).collect().toSeq)

  }

  @Test
  def testRatioOfTheAttendance_when_GiveAValiddataFrame()={

    val expectedSchema = List(
      StructField("Year",IntegerType, true),
      StructField("Male",StringType, true),
      StructField("MaleCount",IntegerType, true),
      StructField("Female",StringType, true),
      StructField("FemaleCount",IntegerType, true),
      StructField("Ratio",DoubleType, true)
    )

    val expectedData=Seq(
      Row(1900,"M",1901,"F",32,59.40625),
      Row(1904,"M",1278,"F",16,79.875),
      Row(1906,"M",1721,"F",11,156.45454545454547)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getRatioOfTheAttendance(df).take(3).toSeq)

  }

  @Test
  def testMostPopularGames_when_GiveAValidDataFrame()={

    val expectedSchema = List(
      StructField("Sport",StringType, true),
      StructField("count",IntegerType, true)
    )

    val expectedData=Seq(
      Row("Athletics",38477),
      Row("Gymnastics",26642)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    Assertions.assertEquals(expectedDF.collect().toSeq,Task.getMostPopularGame(df).collect().toSeq)

  }



}


