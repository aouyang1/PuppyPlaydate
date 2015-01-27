import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object StreamExample {
  def main(args: Array[String]) {

    Logger.getLogger("CheckpointWriter").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("stream_example")
    val ssc = new StreamingContext(conf, Seconds(1))
       
    ssc.checkpoint("/user/PuppyPlaydate/spark_streaming")

    val zkQuorum = "localhost:2181"
    val groupID = "rt"
    val topics = Map("messages" -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupID, topics)

    //val window_count = kafkaStream.countByWindow(Seconds(10), Seconds(2))
    val parsed_message = kafkaStream.map( x => parse(x._2) )
    val messages_by_county = parsed_message.map( message => (compact(render( message \ "state" )) + "," + compact(render( message \ "county" )), 1) )
    val window_count_10sec = messages_by_county.reduceByKeyAndWindow( (a:Int, b:Int) => a + b, Seconds(10), Seconds(1) )
    
    val mapped_window_count_10sec = window_count_10sec.map( tup => {val state_county = tup._1.split(",")
 								   (System.currentTimeMillis, state_county(0).tail.dropRight(1), state_county(1).tail.dropRight(1), tup._2)}) 

    // look for minimum timestamp and remap that tuple section
    mapped_window_count_10sec.print()
    

    
    ssc.start()
    ssc.awaitTermination()
  }

}
