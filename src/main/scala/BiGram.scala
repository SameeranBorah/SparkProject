import org.apache.spark.{SparkConf, SparkContext}
object BiGram {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("BiGram Word Count")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val input = sc.textFile("src/main/resources/Shakespeare.txt")
    input.map{
      _.split('.').map
      {
        substrings =>
          substrings.trim.split(' ').
            map{_.replaceAll("""\W""", "").toLowerCase()}.
            sliding(2)
      }.flatMap{identity}.map{_.mkString(" ")}.
        groupBy{identity}.mapValues{_.size}
    }.
      flatMap{identity}.
      reduceByKey(_+_).
      saveAsTextFile("src/main/resources/outfile_bigram")
    println("OK")
  }

}
