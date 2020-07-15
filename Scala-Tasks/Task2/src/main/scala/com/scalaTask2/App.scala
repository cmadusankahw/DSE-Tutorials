package com.scalaTask2

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.immutable.HashMap

object App extends App {



  var lines= io.Source.fromFile("D:\\Zone24x7\\Scala_Task2\\src\\main\\scala\\inputs\\airports.text").getLines().toList


  val countOfAirportsInIreLand=findAllTheAirportsInGivenCountry("\"Ireland\"").size;
  println(s"countOfAirportsInIreLand: $countOfAirportsInIreLand")

  val countLatitudeGreaterThan40=getCountLatitudeGreaterThan40().size
  println(s"countLatitudeGreaterThan40 : $countLatitudeGreaterThan40")

//  getCountLatitudeGreaterThan40().take(10).foreach(println)

  val fileCountriesLatGreaterThan40 = new File("D:\\Zone24x7\\Scala_Task2\\src\\main\\scala\\outputs\\airports_by_latitude.text")
  var bw = new BufferedWriter(new FileWriter(fileCountriesLatGreaterThan40))

  for((airport,lat) <- getCountLatitudeGreaterThan40()){
     bw.write(airport+"\t\t"+lat+"\n")
  }

  val fileAirportsInUSA=new File("D:\\Zone24x7\\Scala_Task2\\src\\main\\scala\\outputs\\airports_in_usa.text")
  bw=new BufferedWriter(new FileWriter(fileAirportsInUSA))

  for((airport,city)<-findAllTheAirportsInGivenCountry("\"United States\"")){
    bw.write(airport+"\t\t"+city+"\n")
  }

  bw.close()

  groupAirportsByCountry().foreach(println)





//  def getCountOfAirportsInIreLand():Int={
//
//  var count:Int=0
//    for (line <- lines) {
//      val cols= line.split(",").map(_.trim)
//      if(cols(3).equals("\"Ireland\"")) count+=1
//
//    }
//
//
//
//    return count
//
//  }

  def getCountLatitudeGreaterThan40():HashMap[String,Double]={

    var countryMap=new HashMap[String,Double];
    for (line <- lines) {
      val cols = line.split(",").map(_.trim)

      try {

        if (cols(6).toDouble > 40.0) {
          cols(1) = cols(1).replaceAll("[^A-Za-z]+", "")
          countryMap=countryMap+(cols(1)->cols(6).toDouble)
        }

      }catch {
        case e:NumberFormatException=>
      }
    }

    return countryMap;

  }

  def findAllTheAirportsInGivenCountry(country:String):HashMap[String,String]={

    var countryMap=new HashMap[String,String];
    for (line <- lines) {
      val cols = line.split(",").map(_.trim)

      if(cols(3)==country) {
        cols(2)=cols(2).replaceAll("[^A-Za-z]+", "")
        cols(1)=cols(1).replaceAll("[^A-Za-z]+", "")

        countryMap+=(cols(1)->cols(2))
      }

    }

    return countryMap;

  }

  def groupAirportsByCountry(): HashMap[String,List[String]] ={
    var countryMap=new HashMap[String,List[String]];
    for (line <- lines) {
      val cols = line.split(",").map(_.trim)

      if(countryMap.contains(cols(3))){

        var l=countryMap(cols(3)):+cols(1)
        countryMap+=(cols(3)->l)
      }
      else{
        countryMap+=(cols(3)->List(cols(1)))
      }
    }

    return countryMap

  }

  }
