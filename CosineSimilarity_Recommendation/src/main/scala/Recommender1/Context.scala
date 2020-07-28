package Recommender1

import org.apache.spark.sql.SparkSession

object Context {

  def getCtx() : SparkSession = {
    SparkSession.builder.
      master("local[*]")
      .appName("Recommendation System")
      .getOrCreate()

  }

}
