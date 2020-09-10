package Scala_tasks.exercise01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CharacterCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("characterCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // read lines of
    val lines = sc.textFile("in/word_count.text")
    // split into words using flatmap()
    val words = lines.flatMap(line => line.split(" "))
    // split into characters using map()
    val chars = words.map(word => word.split(""))

    // convert to an array with collect() and get length
    val charCount = chars.collect().length
    println("Total no of Characters: " + charCount)
  }
}
