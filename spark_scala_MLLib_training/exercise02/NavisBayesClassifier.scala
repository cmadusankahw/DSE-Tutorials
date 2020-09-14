package Spark_MLLib_tasks.exercise02

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession


object NavisBayesClassifier {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("RandomForest-Credits").master("local[2]").getOrCreate()

    val creditDf = session.read.format("csv")
      .option("header", value = false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .load("in/german.data-numeric")
      .cache()
    creditDf.printSchema()

    // describe basic statistics on the selected column
    creditDf.describe("balance").show()

    // Model Goal: predict a given account is creditable or not

    // columns that need to added to feature column
    val cols = Array("balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPercent", "sexMarried",
      "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone",
      "foreign")

    // VectorAssembler to add feature column
    // input columns - cols
    // feature column - features
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")

    val featureDf = assembler.transform(creditDf)
    featureDf.printSchema()

    // StringIndexer define new 'label' column with 'result' column
    val indexer = new StringIndexer()
      .setInputCol("creditability")
      .setOutputCol("label")
    val labelDf = indexer.fit(featureDf).transform(featureDf)
    labelDf.printSchema()

    // Building RNaive Bayes Model Model

    // split data set training and test
    // training data set - 70%
    // test data set - 30%
    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train Random Forest model with training data set
    val naiveBayesModel = new NaiveBayes(uid = "nb").
      setModelType("multinomial").
      setThresholds(Array(0.4)).
      setFeaturesCol("features").
      setLabelCol("label").fit(trainingData)


    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = naiveBayesModel.transform(testData)
    predictionDf.show(10)


    // evaluate model with area under ROC
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    // measure the accuracy
    val accuracy = evaluator.evaluate(predictionDf)
    println(accuracy)



    // saving and loading model
    // save model
    naiveBayesModel.write.overwrite()  // overwrite if the model is already saved
      .save("out/models/credit-model-naive-bayes")

    // load CrossValidatorModel model here
    val randomForestModelLoaded = CrossValidatorModel
      .load("out/models/credit-model-naive-bayes")

  }
}
