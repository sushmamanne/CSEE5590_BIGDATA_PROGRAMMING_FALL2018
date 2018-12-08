import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.RegressionMetrics

object Creditd {

  // Creating Credit case class for holding input data structure
  case class Creditd(
                     creditability: Double,
                     balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
                     savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
                     residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
                     credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
                   )

  // Parsing and converting all the columns to double type
  def parseCredit(line: Array[Double]): Creditd = {
    Creditd(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LoanPredictionDecisionTree").setMaster("local")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.OFF)


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //Loading csv file as rdd, parsing and then converting to Dataframe
    val creditDF = parseRDD(sc.textFile("src/main/scala/germancredit.csv")).map(parseCredit).toDF().cache()
    creditDF.registerTempTable("credit")
    creditDF.printSchema

    //printing dataframe
    creditDF.show

    //calcualting avg balance, amount, duration for people who are creditable
    sqlContext.sql("SELECT creditability, avg(balance) as avgbalance, avg(amount) as avgamt, avg(duration) as avgdur  FROM credit GROUP BY creditability ").show

    // analysis of balance column
    creditDF.describe("balance").show
    creditDF.groupBy("creditability").avg("balance").show

    val featureCols = Array("balance", "duration", "history", "purpose", "amount",
      "savings", "employment", "instPercent", "sexMarried", "guarantors",
      "residenceDuration", "assets", "age", "concCredit", "apartment",
      "credits", "occupation", "dependents", "hasPhone", "foreign")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(creditDF)
    df2.show

    val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    df3.show
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)

    //applying Decision Tree classifier with depth 3
    val classifier = new DecisionTreeClassifier().setImpurity("gini").setMaxDepth(3).setSeed(5043)
    val model = classifier.fit(trainingData)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val predictions = model.transform(testData)
    model.toDebugString

    //accuracy before adding pipeline
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy before pipeline fitting" + accuracy)

    val regmet = new RegressionMetrics(
      predictions.select("prediction", "label").rdd.map(x =>
        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    )

    //printing all the errors and variance
    println("MeanSquErr: " + regmet.meanSquaredError)
    println("MeanAbsolError: " + regmet.meanAbsoluteError)
    println("RootMeanSqrErr Squared: " + regmet.rootMeanSquaredError)
    println("R Squared: " + regmet.r2)
    println("Explained Variance: " + regmet.explainedVariance + "\n")

    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.maxBins, Array(25, 31))
      .addGrid(classifier.maxDepth, Array(5, 10))
      .addGrid(classifier.impurity, Array("entropy", "gini"))
      .build()

    val steps: Array[PipelineStage] = Array(classifier)
    val pipeline = new Pipeline().setStages(steps)

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val pipelineFittedModel = cv.fit(trainingData)

    //accuracy after pipeline fitting
    val predictions2 = pipelineFittedModel.transform(testData)
    val accuracy2 = evaluator.evaluate(predictions2)
    println("accuracy after pipeline fitting" + accuracy2)

    println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))

    pipelineFittedModel
      .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      .stages(0)
      .extractParamMap

    val regmet2 = new RegressionMetrics(
      predictions2.select("prediction", "label").rdd.map(x =>
        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    )

    //printing all the errors and variance
    println("MeanSquErr: " + regmet2.meanSquaredError)
    println("MeanAbsolError: " + regmet2.meanAbsoluteError)
    println("RootMeanSqrErr Squared: " + regmet2.rootMeanSquaredError)
    println("R Squared: " + regmet2.r2)
    println("Explained Variance: " + regmet2.explainedVariance + "\n")

  }
}