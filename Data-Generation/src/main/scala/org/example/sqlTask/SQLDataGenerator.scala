package org.example.sqlTask

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SQLDataGenerator extends App {

  val sc=new SparkContext("local[*]","Random Data Generation")
  val spark = SparkSession.builder().appName("Random SQL Data Generation").getOrCreate()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val churnLevel = List("null","high risk","moderate","low risk")

  var size:Int=100

  def generateOfferForEachLevels(recencyId : Int,monetaryId:Int,frequencyId:Int): Int ={
    var pair:List[Int] = List(recencyId,monetaryId,frequencyId)
    var pairs = generateOfferFactorials()
    return pairs.indexOf(pair)

  }

  //Write Number of Possible pairs to a List
  def generateOfferFactorials():ListBuffer[List[Int]]={
    var pairs=new ListBuffer[List[Int]]()
    for(i<- 1 to 3){
      for(j<-1 to 3){
        for(k<-1 to 3){
          var pair:List[Int]=List(i,j,k)
          pairs+=pair
        }
      }
    }

   return pairs
  }
  def generateFinalOffer(churnLevel:Int,recencyId : Int,monetaryId:Int,frequencyId:Int): String = {

    val offers = List("null", "Buy 1 Get 1", "Buy 1 Get 2", "Buy 1 Get 3")

    if (churnLevel != 0) {
      return offers(churnLevel)
    } else {
      var numItems: Int = generateOfferForEachLevels(recencyId,monetaryId,frequencyId)
      return "Buy " + numItems + " get 1"
    }
  }

  def generateFinalOfferDescription(churnLevel:Int,recencyId : Int,monetaryId:Int,frequencyId:Int):String={
    val offerDescription = List("No offer","Buy 1 from Item1 Get 1 from Item2","Buy 1 from Item1 Get 2 from Item2","Buy 1 from Item1 Get 3 from Item2")
    if(churnLevel!=0){
      return offerDescription(churnLevel)
    }else{
      var numItems: Int = generateOfferForEachLevels(recencyId,monetaryId,frequencyId)
      return "Buy " + numItems + " from Item1 Get 1 from Item2"
    }
  }

 def generateRandomId(churnLevel:Int):Int={
   var id:Int=0
   if(churnLevel==0){

     while(id==0)
       id =Random.nextInt(4)
   }
   return id

 }

  val recencyCategories = List("null","most recent","moderate","least recent")
  val monetaryCategory = List("null","highest spent","moderate","least spent")
  val frequencyCategories = List("null","most frequent","moderate","least frequent")

  var randomId = 0
  var frequencyId=0
  var recencyId=0
  var monetaryId=0

  var df_for_unique_offer_for_each_churn_level = (1 to size)
    .map(id => (id.toLong,{
      randomId = Random.nextInt(4)
      churnLevel(randomId)
    },{
      recencyId = generateRandomId(randomId)
      recencyCategories(recencyId)
    }, {
      monetaryId = generateRandomId(randomId)
      monetaryCategory(monetaryId)
    },{
      frequencyId =generateRandomId(randomId)
      frequencyCategories(frequencyId)
    },
      generateFinalOffer(randomId,recencyId,monetaryId,frequencyId)
    ,generateFinalOfferDescription(randomId,recencyId,monetaryId,frequencyId)))
    .toDF("id","churn_level","recency_category","monetary_category","frequency_category","offer_name","offer_description")


  df_for_unique_offer_for_each_churn_level.show(20)
  
}
