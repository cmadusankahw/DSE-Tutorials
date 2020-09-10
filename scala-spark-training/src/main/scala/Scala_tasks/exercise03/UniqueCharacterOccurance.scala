package Scala_tasks.exercise03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UniqueCharacterOccurance {
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

    // method 1: get unique character counts by converting to a pair RDD
    val charsPairRdd = chars.map(word => (word, 1))
    // run reduceByKey() transaction to get the count (no of occurances of each unique characters(keys))
    val charCountPairs = charsPairRdd.reduceByKey((x, y) => x + y)

    // get the total no of unique characters (keys in the pair RDD) using count() method
    val charCount = charCountPairs.count()

    // printing results
    println("Total no of Unique Characters: " + charCount)
    println("\nUnique Character Counts: \n")
    for ((word, count) <- charCountPairs.collect()) println(word + " : " + count)

    // save result into a file
    charCountPairs.saveAsTextFile("out/unique_char_counts.text")
  }
}
