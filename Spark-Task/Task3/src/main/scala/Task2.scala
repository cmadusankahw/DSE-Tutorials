import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Task2 extends App {

  val sc = new SparkContext("local[*]","Task2")

  var path = "/home/yasasm/Desktop/ZoneProjects/Spark-scala-CPLiyanage/in/clickstream.csv"

  getCountByGivenSimpleMethodRdd(path,0)

  getCountByGivenMultipleMethodsRdd(path,2,3)

  def getCountByGivenSimpleMethodRdd(path:String,index:Int): Array[(String, Int)] ={
    val rddFromFile = sc.textFile(path)
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    val count= rdd.map(f=>(f(index),1)).reduceByKey((x,y)=>(x+y)).collect()
    return count
  }

  def getCountByGivenMultipleMethodsRdd(path:String,index:Int,index2:Int): Array[((String, String), Int)] ={
    val rddFromFile = sc.textFile(path)
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    val count = rdd.map(f=>((f(index),f(index2)),1)).reduceByKey((x,y)=>(x+y)).collect()
    return count
  }

  def getTopFiveProducts(path:String): Array[(String, Int)] ={
    val rddFromFile = sc.textFile(path)
    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    val count= rdd.map(f=>(f(2),1)).reduceByKey((x,y)=>(x+y)).sortBy(_._2,false).take(5)
    return count
  }

    getTopFiveProducts(path).foreach(println)

}
