import org.apache.spark.{SparkConf, SparkContext}

object ListOfNumbers {
  def g(v:Int) = List(v)
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("List Of Numbers")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val x = sc.parallelize(List(1, 2, 3, 4))
    val evens = x.filter(_ % 2 == 0)
    val double=evens.map(_ * 2)
    val newList=double++x
    newList.collect.foreach(println)
    val n=newList.flatMap(x => g(x));
    val product=newList.reduce( (a: Int, b: Int) => a * b )
    println(product)
  }
}
