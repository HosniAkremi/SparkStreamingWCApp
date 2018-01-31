import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object SparkStreamingWC {
  def main(args: Array[String]): Unit =
{
  val executionMode = args(0)
  val conf =  new SparkConf().setAppName("Simple WC Streaming App").setMaster(executionMode)
  val ssc = new StreamingContext(conf, Seconds(5))
  val lines = ssc.socketTextStream(args(1), args(2).toInt)
  val words = lines.flatMap(x => x.split(" "))
  val parsed_words = words.map(x => (x, 1))
  val words_count = parsed_words.reduceByKey((acc, value) => acc + value)
  words_count.print()
  ssc.start()
  ssc.awaitTermination()
}
}
