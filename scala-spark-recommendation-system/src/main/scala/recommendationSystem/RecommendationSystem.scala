package recommendationSystem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecommendationSystem {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("ClickStreamDataFrames").master("local[2]").getOrCreate()
    import session.implicits._

    // loading Sales Data
    val salesDf =  createSalesDf(session)

    // loading sales Leads Data
    val salesLeadsDf = createLeadsDf(session)

    // creating a Dataframe for Ratings Data
    val ratingsDf: DataFrame = salesDf.map( salesOrder =>
      Rating( salesOrder.getInt(0),
        salesOrder.getInt(2),
        salesOrder.getInt(4)
      ) ).toDF("user", "item", "rating")

    // creating user vs. product dataframe using Sales Leads dataframe
    val customerLeadsPairDf: DataFrame = salesLeadsDf.map(salesLead => ( salesLead.getInt(0), salesLead.getInt(2) ))
      .toDF("user","item")

    // have a look at created dataframes
    salesDf.printSchema()
    salesLeadsDf.printSchema()
    ratingsDf.show()
    customerLeadsPairDf.show()

    // creating a RDD with Ratings Dataframe
    val ratingsRDD: RDD[Rating] = ratingsDf.rdd.map( row => Rating( row.getInt(0), row.getInt(1), row.getDouble(2) ) )
    println("Ratings RDD: " + ratingsRDD.take(10).mkString(" ") )

    // creating a RDD with customerLeads Data Frame
    val customerLeadsRDD: RDD[(Int, Int)] = customerLeadsPairDf.rdd.map(row =>
      (row.getInt(0),
        row.getInt(1))
    )

    // Training Dataset

    // We will be using the ALS algorithm from Spark MLLib to create and train a MatrixFactorizationModel,
    // which takes an RDD[Rating] object as input
    val ratingsModel: MatrixFactorizationModel = ALS.train(ratingsRDD,
      6, /* THE RANK */
      10, /* Number of iterations */
      15.0 /* Lambda, or regularization parameter */
    )

    // generating predictions for Future Ratings
    val salesRecs: RDD[Rating] = ratingsModel.predict(customerLeadsRDD).distinct()

    // printing predicted results
    println("Future Ratings:")
    println( salesRecs.foreach(rating =>
      { println( "Customer: " + rating.user + " Product:  " + rating.product + " Rating: " + rating.rating ) } ) )
  }

  // loading sales Data into a DataFrame
  def createSalesDf(session: SparkSession): DataFrame = {
    session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .csv("in/sales_orders.csv").toDF("UserId","UserName","ProductId","ProductName","Rate","Quantity","Amount")
      .cache()
  }

  // loading sales Leads Data into a DataFrame
  def createLeadsDf(session: SparkSession): DataFrame = {
    session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .csv("in/sales_leads.csv").toDF("UserId","UserName","ProductId","ProductName")
      .cache()
  }
}

