import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD._
object SortArray {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Sort Array")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq(("India",91),("USA",1),("Brazil",55),("Greece",30),("China",86),("Sweden",46),("Turkey",90),("Nepal",977)))
    val rdd2 = rdd1.sortByKey()
      rdd2.collect().foreach(println);
  }
}
