import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame


object icp6 {

    def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      val spark = SparkSession
        .builder()
        .appName("Graph Frames")
        .config("spark.master", "local")
        .getOrCreate()

      val s = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5" +
          "/trip_data.csv")



      val t = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5" +
          "/station_data.csv")


      val input = t.select("name","landmark", "lat", "long", "dockcount").withColumnRenamed("name","id")
      //input.show()
      val output = s.select("StartStation","EndStation","Duration").withColumnRenamed("StartStation","src")
        .withColumnRenamed("EndStation","dst").withColumnRenamed("Duration","relationship")
      //output.show()


      val g=GraphFrame(input,output)
      
      // trianglecount
      val TC = g.triangleCount.run()
      TC.select("id" ,"count").show()

      // Shortest Path
      val SP = g.shortestPaths.landmarks(Seq("San Jose Civic Center","Ryland Park")).run
      SP.show()
 
      // Pagerank
      val PR = g.pageRank.resetProbability(0.15).maxIter(10).run()
      PR.vertices.show()
      PR.edges.show()
 
      // BFS
       val BFS = g.bfs.fromExpr("id = 'Mezes Park'").toExpr("dockcount < 15").run()
       BFS.show()


      g.vertices.write.csv("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5/vertices3")
      g.edges.write.csv("C:/Users/Sushu/Desktop/BDFiles/M2-ICP5/edges3")








    }

}
