package Spark_MLLib_tasks.exercise04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BasicStatistics {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Basic-Statistics").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val observations: RDD[Vector] = sc.parallelize(
    Seq(
      Vectors.dense(1.0, 10.0, 100.0),
      Vectors.dense(2.0, 20.0, 200.0),
      Vectors.dense(3.0, 30.0, 300.0)
    )
  ) // an RDD of Vectors

  // exercise 01: Compute column summary statistics.
  val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  println(summary.mean) // a dense vector containing the mean value for each column
  println(summary.variance) // column-wise variance
  println(summary.numNonzeros) // number of nonzeros in each column

  // correlation of the vector RDD values (coutput --> matrix)
  val correlMatrix: Matrix = Statistics.corr(observations, "pearson")
  println(correlMatrix.toString)


  // exercise 02 : Correlation
  val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))
  // must have the same number of partitions and cardinality as seriesX
  val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

  // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
  // method is not specified, Pearson's method will be used by default.
  val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
  println(s"Correlation is: $correlation")


  // exercise 03: hypothesis testing & matrix operations
  // a contingency matrix. Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
  val mat: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

  // conduct Pearson's independence test on the input contingency matrix
  // summary of the test including the p-value, degrees of freedom, test statistic, the method
  // used, and the null hypothesis.
  val independenceTestResult = Statistics.chiSqTest(mat)
  // summary of the test including the p-value, degrees of freedom
  println(s"$independenceTestResult\n")


}
