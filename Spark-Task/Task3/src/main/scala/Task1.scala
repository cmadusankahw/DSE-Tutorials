import scala.collection.mutable
import java.io.File

import scala.collection.immutable.HashMap
import scala.io.Source

object Task1 extends App{

  var lines = Source.fromFile("/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv").getLines().toList

  def countClicksPerGivenSingleMethod(index:Int):HashMap[String,Integer]={

  var clicks = new HashMap[String,Integer]

  for(line <- lines){
    var cols = line.split(",").map(_.trim)

    if(clicks.contains(cols(index))){
      val count = clicks(cols(index))+1
      clicks = clicks+(cols(index)->count)
    }else{
      clicks = clicks + (cols(index)->1)
    }
  }
    return clicks
  }

  def countClicksPerGivenAnyMultipleMethods(index1:Int,index2:Int):HashMap[List[String],Integer]={

    var clicks = new HashMap[List[String],Integer]

    for(line <-lines){
      var cols = line.split(",").map(_.trim)

      var given = List(cols(index1),cols(index2))

      if(clicks.contains(given)){
        val count = clicks(given)+1
        clicks = clicks+(given->count)
      }else{
        clicks = clicks + (given -> 1)
      }

    }
    return clicks
  }

  def getTopFiveProducts():HashMap[String,Integer]={
    var clicks = HashMap(countClicksPerGivenSingleMethod(2).toSeq.sortWith(_._2 < _._2):_*)
    return clicks.take(5)
  }

//  countClicksPerGivenSingleMethod(2).foreach(println)
  getTopFiveProducts().foreach(println)

}
