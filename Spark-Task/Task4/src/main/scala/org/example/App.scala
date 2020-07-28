package org.example

import com.andrewmccall.faker.Faker
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

import scala.collection.immutable.HashMap
import scala.util.Random


object App extends App {

  val log=Logger.getLogger(getClass.getName)

  val sc=new SparkContext("local[*]","Task2")
  val spark = SparkSession.builder().appName("Task2").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  var df = (1 to 100)
    .map(id => (id.toLong,Math.abs(Random.nextLong % 100L),Math.abs(Random.nextLong%100000L)))
    .toDF("id","age","salary")

  df= df.withColumn("gender", lit(""))

  df = df.withColumn( "gender",
      when(col("id").cast("int")%2===1, lit("Male") )
        .otherwise(lit("Female"))
    )

  def generatedNames(df:DataFrame):DataFrame={

    var dataFrameSize=df.count()

    var namesMap=new HashMap[Long,String]

    for( a <- 1L to dataFrameSize){
      val faker = new Faker()
      val name= faker("name.name")

      namesMap=namesMap+(a->name)
    }

    val df_names = namesMap.toSeq.toDF("nameid", "name")
    df_names
  }

  generatedNames(df).show()

  var dfNames=generatedNames(df)
  df=df.join(dfNames,dfNames("nameid")===df("id"))


  def getSummaryOfDescriptiveStatistics(df:DataFrame)={
    df.describe().show()
  }

  def meanOfGivenNumericColumn(df:DataFrame,colName:String)={
    if(df.columns.contains(colName)) {
      df.select(mean(colName)).show()
    }else{
      throw new Exception("Column in not present in the data frame")
    }
  }

  def getMaxofGivenNumericColumn(df:DataFrame,colName:String)={
    if(df.columns.contains(colName)) {
        df.select(max(colName)).show()
    }else{
      throw new Exception("Column in not present in the data frame")
    }
  }

  def getMinOfGivenNumericColumn(df:DataFrame,colName:String)={
    if(df.columns.contains(colName)) {
        df.select(min(colName)).show()
    }else{
    throw new Exception("Column in not present in the data frame")
    }
  }

  def getCovarianceOfGivenTwoColumns(df:DataFrame,col1:String,col2:String): Double ={

    if(df.columns.contains(col1)&&df.columns.contains(col2)) {
      return df.stat.cov(col1, col2)
    }else{
      throw new Exception("Columns are not present in the data frame")
    }
  }

  def getCorrelationBetweenGivenTwoColumns(df:DataFrame,col1:String,col2:String):Double={
    if(df.columns.contains(col1)&&df.columns.contains(col2)) {
      return df.stat.corr(col1,col2)
    }else{
      throw new Exception("Columns are not present in the data frame")
    }
  }

  def getGenderWiseFrequeancyDistribution(df:DataFrame)={
   df.groupBy("gender").count().show()
  }

  def getAgeWiseFrequenacyDistribution(df:DataFrame,age:Int)={
    var df2 = df.withColumn( "age",
      when(col("age").cast("int")>=age, lit(s"Above $age") )
        .otherwise(lit(s"Below $age"))
    )

    df2.groupBy("age").count().show()
  }

  def sortByGivenColumn(df:DataFrame,colName:String)={
    if(df.columns.contains(colName)) {
      df.orderBy(desc(colName)).show()
    }else{
      throw new Exception("Column is not present in the data frame")
    }
  }

def filterDetailsByGivenSalary(df:DataFrame,salary:Int): Unit ={
  var df2 = df.withColumn( "salary",
    when(col("salary").cast("int")>=salary, lit(s"Above $salary") )
      .otherwise(lit(s"Below $salary"))
  )

  df2.groupBy("salary").count().show()
}

  def encodeGivenColumns(df:DataFrame,colName:String)={

    if(df.columns.contains(colName)) {

      var indexer = new StringIndexer().setInputCol(colName).setOutputCol("Indexed").fit(df).transform(df)
      indexer.show()
    }else{
      throw new Exception("Column is not present in the data frame")
    }
  }

  def getCrossTabulationForGivenColumns(df:DataFrame,col1:String,col2:String)={
    if(df.columns.contains(col1)&&df.columns.contains(col2)) {
        df.stat.crosstab(col1, col2).show()
    }else{
      throw new Exception("Columns are not present in the data frame")
    }
  }

  def assembledGivenColumnsIntoOne(df:DataFrame,col1:String,col2:String)={
    if(df.columns.contains(col1)&&df.columns.contains(col2)) {
        var assembled = new VectorAssembler().setInputCols(Array(col1,col2)).setOutputCol("features").transform(df)
        assembled.show()
    }else{
      throw new Exception("Columns are not present in the data frame")
    }
  }


  def maxSalaryAgeGroup(df:DataFrame)={
    var table = df.registerTempTable("table")
    var max= df.groupBy("age").max("salary")
    max.show()
  }


  def filterByGivenColumnAndValue(df:DataFrame,colName:String,value:Int)={
    if(df.columns.contains(colName)) {
      var df2 = df.filter(col(colName).cast("int") >= value)
      df2.show()
    }else{
      throw new Exception("Column is not present in the data frame")
    }
  }

  def getAverageSalaryByGender(df:DataFrame)={

    var averageSal = df.groupBy("gender").avg("salary")
    averageSal.show()
  }

  def getRatioSalaryByGender(df:DataFrame)={
    var averageSalMen = df.groupBy("gender").avg("salary").where(col("gender")==="Male")
    var averageSalWomen = df.groupBy("gender").avg("salary").where(col("gender")==="Female")

    averageSalMen = averageSalMen.withColumnRenamed("gender","male")
        .withColumnRenamed("avg(salary)","MenSal")
        .withColumn("id",lit("1"))

    averageSalWomen=averageSalWomen.withColumnRenamed("gender","female")
      .withColumnRenamed("avg(salary)","FemaleSal")
        .withColumn("id",lit("1"))

    var joinedDf= averageSalMen.join(averageSalWomen,"id")
    var ratio = joinedDf.withColumn("Ratio",col("MenSal")/col("FemaleSal"))
    ratio.show()
  }

  def ratioBetweenGivenColumnMinaAndMx(df:DataFrame,colName:String)={

    if(df.columns.contains(colName)) {
      var maxDf = df.select(max(colName)).withColumn("id", lit("1"))
      var minDf = df.select(min(colName)).withColumn("id", lit("1"))

      var ratio = maxDf.join(minDf, "id").withColumn("Ratio", col(s"max($colName)") - col(s"min($colName)"))
      ratio.show()
    }else{
      throw new Exception("Column in not present in the d ata frame")
    }
  }
  
}



