import org.apache.spark.{SparkConf, SparkContext}

object SortArray {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Sort Array")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val collection = Array(3, 6, 5, 9, 2, 8, 1, 4, 10)
    val rdd = sc.parallelize(collection)
    //val rdd1 = sc.parallelize(Seq(("India",91),("USA",1),("Brazil",55),("Greece",30),("China",86),("Sweden",46),("Turkey",90),("Nepal",977)))
    val rdd1 = rdd.map(x => (x, x)).sortByKey(false).map(_._1)
    rdd1.collect().foreach(println);
  }
}
