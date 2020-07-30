package uber

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}

object UberCluster extends App {

  val sc = new SparkContext("local[*]","Uber Trip Analysis")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val schema = new StructType()
    .add("time",TimestampType,true)
    .add("lat",DoubleType,true)
    .add("lon",DoubleType,true)
    .add("base",StringType,true)

  var df = sqlContext.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .option("delimiter", ",")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
    .load("D:\\Projects\\Uber Trip Analysis\\src\\main\\scala\\inputs\\uber.csv")

  df.printSchema()

  df.describe().show()

  df.limit(10).show()

  df = df.na.drop()

  df.groupBy("base").count().show()

  df.groupBy("time").count().show()

  df.groupBy("lat","lon").count().show()


//  Build Clustering Model


//  Set Feature Columns
  val featureColumns = Array("lat","lon")

//  Assemble feature columns in to data frame as a single column

  val vectorAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")

  val assembledDataFrame = vectorAssembler.transform(df)

  assembledDataFrame.printSchema()
  assembledDataFrame.limit(10).show()

  //    Split Data for Training and Testing
  var splits=assembledDataFrame.randomSplit(Array(0.8,0.2),seed =12345)
  var trainingSet=splits(0)
  var testingSet=splits(1)

//  Build K-Means Clustering Model

  val kMeans = new KMeans()
    .setFeaturesCol("features")
    .setK(8)
    .setPredictionCol("prediction")

  val model = kMeans.fit(trainingSet)

  model.clusterCenters.foreach(println)

  var predictions = model.transform(trainingSet)

  predictions.limit(20).show()

  predictions.groupBy("prediction").count().show()

  // save model
  model.write.overwrite().save("uber_cluster.model")


  println("\n\n\n\n\n\n\n\n\n\t\t\t\t\t\t\tStart Loading Model")
//  Load Model
  var loadModel = KMeansModel.load("uber_cluster.model")


//  Use Model to detect clusters

  val sampleDF = Seq(
    ("5/1/2014 0:02:00", 41.7521, 75.9914, "B02512"),
    ("5/1/2014 0:06:00", 42.6965, -73.9715, "B02512"),
    ("5/1/2014 0:15:00", 43.7464, 75.9838, "B02512"),
    ("5/1/2014 0:17:00", 44.7463, 74.0011, "B02512"),
    ("5/1/2014 0:17:00", 45.7594, -76.9734, "B02512")
  ).toDF("time", "lat", "lon", "base")


//  Assemble data frame

  var sampleAssembled = vectorAssembler.transform(sampleDF)

  val samplePredictions = loadModel.transform(sampleAssembled)

  samplePredictions.show()


}
