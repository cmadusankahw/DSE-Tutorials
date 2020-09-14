package Spark_MLLib_tasks.exercise02

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession


object RandomForestClassifier {
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

    // Building Random Forest Model
    // When building and training the Random Forest classifier model we need to specify maxDepth, maxBins, impurity, auto and seed parameters.

    // maxDepth: Maximum depth of a tree. Increasing the depth makes the model more powerful, but deep trees take longer to train.
    // maxBins:  Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node.
    // impurity: Criterion used for information gain calculation
    // auto:     Automatically select the number of features to consider for splits at each tree node
    // seed:     Use a random seed number , allowing to repeat the results

    // split data set training and test
    // training data set - 70%
    // test data set - 30%
    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train Random Forest model with training data set
    val randomForestClassifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setNumTrees(20)
      .setFeatureSubsetStrategy("auto")
      .setSeed(seed)

    val randomForestModel = randomForestClassifier.fit(trainingData)
    println(randomForestModel.toDebugString) // printing Random Forest Tree (Debug Level)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = randomForestModel.transform(testData)
    predictionDf.show(10)

    ///////////////////////////////////////////////////////////////////////////////////////////

    // building same model with spark pipeline
    // we run marksDf on the pipeline, so split marksDf
    val Array(pipelineTrainingData, pipelineTestingData) = creditDf.randomSplit(Array(0.7, 0.3), seed)

    // VectorAssembler and StringIndexer are transformers
    // LogisticRegression is the estimator
    val stages = Array(assembler, indexer, randomForestClassifier)

    // build pipeline
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(pipelineTrainingData)

    // test model with test data
    val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
    pipelinePredictionDf.show(10)


    /////////////////////////////////////////////////////////////////////////////////////////////

    // evaluate model with area under ROC
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    // measure the accuracy
    val accuracy = evaluator.evaluate(predictionDf)
    println(accuracy)

    // measure the accuracy of pipeline model
    val pipelineAccuracy = evaluator.evaluate(pipelinePredictionDf)
    println(pipelineAccuracy)

    //////////////////////////////////////////////////////////////////////////////////////////////

    // tuning the model
    // parameters that needs to tune, we tune
    //  1. max buns
    //  2. max depth
    //  3. impurity
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
      .addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8))
      .addGrid(randomForestClassifier.impurity, Array("entropy", "gini"))
      .build()

    // define cross validation stage to search through the parameters
    // K-Fold cross validation with BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    // fit will run cross validation and choose the best set of parameters
    // this will take some time to run
    val cvModel = cv.fit(pipelineTrainingData)

    // test cross validated model with test data
    val cvPredictionDf = cvModel.transform(pipelineTestingData)
    cvPredictionDf.show(10)

    //////////////////////////////////////////////////////////////////////////////////////////

    // saving and loading model
    // save model
    cvModel.write.overwrite()  // overwrite if the model is already saved
      .save("out/models/credit-model")

    // load CrossValidatorModel model here
    val randomForestModelLoaded = CrossValidatorModel
      .load("out/models/credit-model")

  }
}
