package com.wordcount

import org.apache.log4j.{BasicConfigurator, Logger}

import scala.collection.immutable.HashMap


object App extends App {

  val log=Logger.getLogger(getClass.getName)
  BasicConfigurator.configure()

  log.info("Start Finding Word Count")
  val inputPath="D:\\Zone24x7\\Word-Count-Training\\Scala\\src\\main\\inputs\\word_count.txt"


  countWords(inputPath).foreach(println)
  log.info("Successfully Print the Word Counts")


  def countWords(filePath:String):HashMap[String,Int]={

    var wordMap=new HashMap[String,Int]

    try {

      log.info("Start Read the given text file")
      val file = scala.io.Source.fromFile(filePath)

      log.info("Read all the Lines in the File")
      for (line <- file.getLines()) {

          var replacedLine=line.replaceAll("[^A-Za-z]+", " ").replaceAll("\n"," ").toLowerCase
          var words=replacedLine.split(" ")

        for(word<- words){

          if(wordMap.contains(word)){
            val count=wordMap(word)+1
            wordMap=wordMap+(word->count)
          }else{
            wordMap=wordMap+(word->1)
          }
        }
      }

    }catch{

      case e:java.io.FileNotFoundException=>{

        log.info("Given File is not found")

      }
    }
    log.info("Returning the Occurrences of each word")
    return wordMap;

  }
}
