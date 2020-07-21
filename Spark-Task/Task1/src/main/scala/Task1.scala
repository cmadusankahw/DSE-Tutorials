import org.apache.spark.SparkContext

object Task1 extends App{

  val sc=new SparkContext("local[*]","Task1")
  var lines = sc.textFile("/home/yasasm/Desktop/ZoneProjects/spark-tasks/src/main/inputs/spark-task1");
  var words = lines.flatMap(line => line.split(' '))
  var wordsMaped = words.map(x => (x,1))
  var count = wordsMaped.reduceByKey((x,y) => x + y).sortBy(x=>x._2,ascending = false).take(10)
  count.foreach(println)

}
