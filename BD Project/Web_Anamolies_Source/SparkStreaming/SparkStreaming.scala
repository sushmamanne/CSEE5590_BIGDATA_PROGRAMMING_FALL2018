import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util._
import org.apache.log4j.{ Level, Logger }

object streamingSpark {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventClient <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumePollingEventClient").setMaster("local[6]")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt)

    val mappedlines = stream.map { sparkFlumeEvent =>
      val event = sparkFlumeEvent.event
      println("Value of event " + event)
      println("Value of event Header " + event.getHeaders)
      println("Value of event Schema " + event.getSchema)
      val messageBody = new String(event.getBody.array())
      println("Value of event Body " + messageBody)
      messageBody
    }.print()

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()

    ssc.start()
    ssc.awaitTermination()
  }

}