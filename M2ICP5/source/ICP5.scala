import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object ip51 {

    def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.master", "local")
        .getOrCreate()

      val s = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5" +
          "/trip_data.csv")
      t.show(10)


      val t = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5" +
          "/station_data.csv")
      s.show(10)

      val input = t.select("name","landmark").withColumnRenamed("name","id")
      input.show()
      val output = s.select("StartStation","EndStation","Duration").withColumnRenamed("StartStation","src").withColumnRenamed("EndStation","dst").withColumnRenamed("Duration","relationship")
      output.show()


      val g=GraphFrame(input,output)

      g.vertices.show()
      g.edges.show()

      val inDeg = g.inDegrees
      inDeg.show(5)

      val outDeg = g.outDegrees
      outDeg.show(5)

      val deg = g.degrees
      deg.show(5)

      val motifs = g.find("(a)-[e]->(b); (b)-[e1]->(c); (c)-[e3]->(a)")
      motifs.show()

      g.vertices.write.csv("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5/vertices1")
      g.edges.write.csv("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5/edges1")



    }

}
