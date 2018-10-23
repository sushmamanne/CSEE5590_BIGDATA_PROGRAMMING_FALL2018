import org.apache.log4j.{Level, Logger}
import org.apache.spark._


object MergeSort {

  def main(args: Array[String]): Unit = {


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Spark Context

    val conf = new SparkConf().setAppName("mergeSort").setMaster("local");
    val sc = new SparkContext(conf);

    val a = Array(9, 14, 16, 7, 1);

    val b = sc.parallelize(Array(9, 14, 16, 7, 1));


    val maparray = b.map(x => (x, 1))

    val sorted = maparray.sortByKey();


    //Printing the RDD before Sort
    sorted.keys.collect().foreach(println)
  }
}
