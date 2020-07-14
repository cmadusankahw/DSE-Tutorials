
import java.io.{BufferedWriter, File, FileNotFoundException, FileWriter, IOException, PrintWriter}

import scala.collection.immutable.HashMap
import scala.io.Source
import org.apache.log4j.{BasicConfigurator, Logger}


object WordCount extends App{

//  Initialize the logger
  val log=Logger.getLogger(getClass.getName)
  BasicConfigurator.configure()

//  Task 1
  log.info("Scala Task-Day1")

  val filePath:String="D:\\Zone24x7\\Scala Tasks\\src\\main\\scala\\inputs\\Scala Task 01.txt"

  var lines:String=""

  log.info("Start Reading the Scala Task 01.txt File")

  try {

    val file = scala.io.Source.fromFile(filePath)

    log.info("Read all the Lines in the File")
    for (line <- file.getLines()) {
      lines = lines + line
    }

  }catch {

    case e:java.io.FileNotFoundException=>{

      log.info("Given File is not found")

    }
  }

//  println(lines)

  log.info("Remove All non alphabet characters and convert all to lower case characters")
  lines = lines.replaceAll("[^A-Za-z]+", "").toLowerCase()

//  println("\n"+lines)

  val characterCount:Int=lines.length
  println(s"Number of alphabetical characters in the File is $characterCount")
  log.info(s"Number of alphabetical characters in the File is $characterCount")


//  Count Unique Characters

  val characterMap=stringToHashMap(lines)
  val numberOfUniqueCharacters=characterMap.size

  println(s"Number of unique alphabetical characters in the File is $numberOfUniqueCharacters")
  log.info(s"Number of unique alphabetical characters in the File is $numberOfUniqueCharacters")

  log.info("Start write characters occurrence ratio")

  var writerPath="D:\\Zone24x7\\Scala Tasks\\src\\main\\scala\\output\\out.txt"

  try{

    var fileWriter=new PrintWriter(new File(writerPath))
    fileWriter.write("Character\t\tRatio\n")

   for((key,value)<-characterMap){

     fileWriter.write(s"$key\t\t\t$value\n")

   }

    log.info("Writing to the file is success")

  }catch {
    case e:IOException=>{
      log.info("Given File is not found")
    }
  }

  log.info("Print Characters and Ratio in the console")
  characterMap.foreach(println)


  log.info("Finished the Task")


//  Method to Put String to a HashMap

  def stringToHashMap(line:String):HashMap[Char,Int]={

    log.info("Add Characters to HashMap")

    var characterMap=new HashMap[Char,Int]

    for(c <- line){
      if(characterMap.contains(c)){

        val count=characterMap(c)+1
        characterMap=characterMap+(c->count)

      }else{
        characterMap=characterMap+(c->1)
      }
    }

    return characterMap

  }


}