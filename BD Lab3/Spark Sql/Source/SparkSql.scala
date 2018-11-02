import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._


// Author Raju Nekadi Sushma Manne


object Fifa {


  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Fifa Spark Dataframe Sql")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    // We are using all 3 Fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema

    val wc_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:/Users/Sushu/Desktop/BDFiles/BigData_Lesson2" +
        "/WorldCups.csv")

    val wcplayers_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:/Users/Sushu/Desktop/BDFiles/BigData_Lesson2" +
        "/WorldCupPlayers.csv")


    val wcmatches_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:/Users/Sushu/Desktop/BDFiles/BigData_Lesson2" +
        "/WorldCupMatches.csv")


    // Printing the Schema

    wc_df.printSchema()

    wcmatches_df.printSchema()

    wcplayers_df.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames

    //First of all creat three Temp View

    wc_df.createOrReplaceTempView("WorldCup")

    wcmatches_df.createOrReplaceTempView("wcMatches")

    wcplayers_df.createOrReplaceTempView("wcPlayers")


    // Find the attendance by years using WorldCup view

    val wcAtd = spark.sql("select Attendance,Year from WorldCup Order By Year")

    wcAtd.show()

    //Find the goals by years using WorldCup view
    val wcgoal = spark.sql("select GoalsScored,Year from WorldCup Order By Year")

    wcgoal.show()

    //Cities that hosted highest world cup matches on view wcMatches

    val cityCount = spark.sql("select Count(City),City from wcMatches Group By City")

    cityCount.show()

    //Teams with the most world cup final victories on WorldCup view

    val CountryWin = spark.sql("select Count(Winner),Winner from WorldCup Group By Winner")

    CountryWin.show()

    // Display all Stage Final Matches

    val FinalDF = spark.sql("select * from wcMatches where Stage='Final'")

    FinalDF.show()

    //No of matches in year 2014

    val match2014 = spark.sql("select count(*) from wcMatches where year=2014")

    match2014.show()

    //Country which hoster World Cup highest number of times

    val CountHost = spark.sql("select Count(Country),Country from WorldCup Group by Country")

    CountHost.show()

    //Stadium with highest number of matches

    val StadmatchCount = spark.sql("select Count(Stadium),Stadium from wcMatches Group By Stadium")

    StadmatchCount.show()

    //HomeTeam Goals Count and HomeTeam Names by Years

    val homeGoals = spark.sql("select HomeTeamName,Count(HomeTeamGoals),Year from wcMatches Group By Year,HomeTeamName")

    homeGoals.show()

    // Away Team Goals

    val awayTeamGoals = spark.sql("select AwayTeamName,Count(AwayTeamGoals),Year from wcMatches Group By Year,AwayTeamName")

    awayTeamGoals.show()


    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.

    // To Solve this Prolblem we first create the rdd as we already have Dataframe wc_df created above code

    // RDD creation

    val csv = sc.textFile("C:/Users/Sushu/Desktop/BDFiles/BigData_Lesson2" +
      "/WorldCups.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()

    //RDD Highest Numbers of goals

    val rddgoals = data.filter(line => line.split(",")(6) != "NULL").map(line => (line.split(",")(1),
      (line.split(",")(6)))).takeOrdered(10)
    rddgoals.foreach(println)

    // Dataframe

    wc_df.select("Country","GoalsScored").orderBy("GoalsScored").show(10)

    // Dataframe SQL

    val dfGoals = spark.sql("select Country,GoalsScored FROM WorldCup order by GoalsScored Desc Limit 10").show()

    // Year, Venue country = winning country

    // Using RDD

    val rddvenue = data.filter(line => line.split(",")(1)==line.split(",")(2))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(2)))
      .collect()

    rddvenue.foreach(println)

    // Using Dataframe

    wc_df.select("Year","Country","Winner").filter("Country==Winner").show(10)

    // usig Spark SQL

    val venueDF = spark.sql("select Year,Country,Winner from WorldCup where Country = Winner order by Year").show()

    // Details of years ending in ZERO

    // RDD
    var years = Array("1930","1950","1970","1990","2010")



    val rddwinY = data.filter(line => (line.split(",")(0)=="1930" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddwinY.foreach(println)

    //DataFrame
    wc_df.select("Year","Winner","Runners-Up").filter("Year='1930' or Year='1950' or " +
      "Year='1970' or Year='1990' or Year='2010'").show(10)

    //DF - SQL

    val winYDF = spark.sql("SELECT * FROM WorldCup  WHERE " +
      " Year IN ('1930','1950','1970','1990','2010') ").show()

    //2014 world cup stats
    //Rdd

    val rddStat = data.filter(line=>line.split(",")(0)=="2014")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddStat.foreach(println)

    //using Dataframe
    wc_df.filter("Year=2014").show()

    //using DF - Sql
    spark.sql(" Select * from WorldCup where Year == 2014 ").show()


    //Max matches played

    //RDD

    val rddMax = data.filter(line=>line.split(",")(8) == "64")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddMax.foreach(println)

    // DataFrame
    wc_df.filter("MatchesPlayed == 64").show()

    // Spark SQL

    spark.sql(" Select * from WorldCup where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WorldCup )" ).show()



  }





}