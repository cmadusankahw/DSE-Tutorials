package Scala_tasks.exercise02

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UniqueCharacterCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("characterCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // read lines of
    val lines = sc.textFile("in/word_count.text")
    // split into words using flatmap()
    val words = lines.flatMap(line => line.split(" "))
    // split into characters using map()
    val chars = words.flatMap(word => word.split(""))

    // method 2: USing countByValue() on a regular RDD to count unique characters
    val uniqueCharCount = chars.countByValue()
    // printing results
    for ((word, count) <- uniqueCharCount) println(word + " : " + count)

  }
}
