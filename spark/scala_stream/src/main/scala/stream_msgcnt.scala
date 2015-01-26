import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

object StreamExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("stream_example")
    val ssc = new StreamingContext(conf, Seconds(1))

    /*
    val lines = ssc.socketTextStream("localhost", 8000)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map( word => (word, 1) )
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()
    */


    val zkQuorum = "localhost:2181"
    val groupID = "rt"
    val topics = Map("messages" -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupID, topics)

    val stream = kafkaStream.map(x => x._2)

    stream.print()
    
    ssc.start()
    ssc.awaitTermination()
}

}
