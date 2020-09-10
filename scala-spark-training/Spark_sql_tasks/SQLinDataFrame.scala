package Spark_sql_tasks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SQLinDataFrame {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val raw_data = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/RealEstate.csv")

    // create spark sql context
    val sql_context = session.sqlContext

    // register temporary table
    raw_data.createOrReplaceGlobalTempView("sample")

    // get the value count using the sql query
    val status = sql_context.sql(" SELECT Status, count(*) as freq from sample GROUP BY Status ")

    // view the results
    status.show()

  }
}
