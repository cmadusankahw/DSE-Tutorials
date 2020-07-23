import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Task2 extends App {

  val sc = new SparkContext("local[*]","Task2")

  var path = "/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv"

   val rddFromFileTest:RDD[String] = sc.textFile(path)

  def getCountByGivenSimpleMethodRdd(rddFromFile:RDD[String],index:Int): Array[(String, Int)] ={
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })
    val count= rdd.map(f=>(f(index),1)).reduceByKey((x,y)=>(x+y)).collect()
    return count
  }

  def getCountByGivenMultipleMethodsRdd(rddFromFile:RDD[String],index:Int,index2:Int): Array[((String, String), Int)] ={
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    val count = rdd.map(f=>((f(index),f(index2)),1)).reduceByKey((x,y)=>(x+y)).collect()
    return count
  }

  def getTopFiveProducts(rddFromFile:RDD[String]): Array[(String, Int)] ={
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    val count= rdd.map(f=>(f(2),1)).reduceByKey((x,y)=>(x+y)).sortBy(_._2,false).take(5)
    return count
  }




}
