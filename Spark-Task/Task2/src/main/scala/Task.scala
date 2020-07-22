
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._


object Task extends App{

  val sc = new SparkContext("local[*]","Task2")
  val sqlContext = new SQLContext(sc)

  def getTotalNumberOfAthletes(df:DataFrame):DataFrame={
    var totalNumberOfAthletes = df.select(countDistinct("ID")).alias("Total Number of Athletes")

    return totalNumberOfAthletes
  }

  def getGoldMedalWinners(df:DataFrame):DataFrame={
    val goldMedalWinners = df.select("ID","Name","Medal").where(col("Medal")==="Gold").distinct().orderBy(col("ID").asc)

    return goldMedalWinners
  }


  def getCountryWithMAxGoldMedals(df:DataFrame):DataFrame={
    val countryWithMaxGoldMedals = df.groupBy("Team","Medal","Year").count()
      .where(col("Medal")==="Gold")
      .select("Team","Year","count").orderBy(desc("count")).limit(1)

    return countryWithMaxGoldMedals
  }

  def getAverageHeightOfMen(df:DataFrame):DataFrame={
    val averageHeightOfMen = df.groupBy("Sex").agg(avg("Height").alias("Average Height of Men"))
      .where(col("Sex")==="M")

    return averageHeightOfMen
  }

  def getAverageWeightOfWomen(df:DataFrame):DataFrame={
    val averageWeightOfWomen = df.groupBy("Sex").agg(avg("Weight").alias("Average weight of Women"))
      .where(col("Sex")==="F")

    return averageWeightOfWomen
  }

  def getRatioOfTheAttendance(df:DataFrame):DataFrame={

    var countForEachYearMale = df.groupBy("Sex","Year").count().where(col("sex")=== "M")
    var countForEachYearFemale = df.groupBy("Sex","Year").count().where(col("Sex")==="F")

    countForEachYearMale =countForEachYearMale.withColumnRenamed("Sex","Male")
      .withColumnRenamed("count","MaleCount")
    countForEachYearFemale =countForEachYearFemale.withColumnRenamed("Sex","Female")
      .withColumnRenamed("count","FemaleCount")

    var joined = countForEachYearMale.join(countForEachYearFemale,"Year")
    var ratio = joined.withColumn("Ratio",col("MaleCount")/col("FemaleCount")).orderBy(col("Year").asc)

    return ratio
  }

  def getMedalDistribution(df:DataFrame):DataFrame={
    var medalDistribution = df.groupBy("Team","Medal").count().select("Team","Medal","count")
    return medalDistribution
  }

  def getMostPopularGame(df:DataFrame):DataFrame={
    var mostPopularGame = df.groupBy("Sport").count().orderBy(col("count").desc).limit(2)
    return mostPopularGame
  }

}
