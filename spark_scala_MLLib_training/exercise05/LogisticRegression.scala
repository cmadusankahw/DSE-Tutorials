package Spark_MLLib_tasks.exercise05

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("LogisticRegression").master("local[2]").getOrCreate()

    // read to DataFrame
    val marksDf = session.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .load(getClass.getResource("in/scores.csv").getPath)
      .cache()
    marksDf.printSchema()

    // describe the score column : basic statistics
    marksDf.describe("score1").show()

    // columns that need to added to feature column
    val cols = Array("score1", "score2")

    // VectorAssembler to add feature column
    // input columns - cols
    // feature column - features
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(marksDf)
    featureDf.printSchema()

    // StringIndexer define new 'label' column with 'result' column
    val indexer = new StringIndexer()
      .setInputCol("result")
      .setOutputCol("label")
    val labelDf = indexer.fit(featureDf).transform(featureDf)

    // split data set training and test
    // training data set - 70%
    // test data set - 30%
    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train logistic regression model with training data set
    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)
    val logisticRegressionModel = logisticRegression.fit(trainingData)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = logisticRegressionModel.transform(testData)
    predictionDf.show(10)

    // evaluate model with area under ROC
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    // measure the accuracy
    val accuracy = evaluator.evaluate(predictionDf)
    println(accuracy)

    // save model
    logisticRegressionModel.write.overwrite()
      .save("out/models/score-model")

    // load model
    val logisticRegressionModelLoaded = LogisticRegressionModel
      .load("out/models/score-model")

  }
}
