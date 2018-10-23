import org.apache.spark.{SparkConf, SparkContext}

object CharCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Character Count")
      .setSparkHome("src/main")
    val sc = new SparkContext(conf)
    val input = sc.textFile("src/main/scala/input.txt")
    val count = input.flatMap(line ⇒ line.split(""))
      .map(char ⇒ (char, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("src/main/scala/outfile1")
    println("OK");

  }
}
