import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.driver.core.utils._


object StreamExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("stream_example").set("spark.cassandra.connection.host", "54.215.184.69")
    val ssc = new StreamingContext(conf, Seconds(5))
       
    ssc.checkpoint("/user/PuppyPlaydate/spark_streaming")

    val zkQuorum = "localhost:2181"
    val groupID = "rt"
    val topics = Map("messages" -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupID, topics)

    def single_to_double(digit: String): String = if (digit.length==1) "0"+digit else digit


    val parsed_message = kafkaStream.map( x => parse(x._2) ).cache()
    
    val messag_counts_by_county = parsed_message.map( message => (compact(render( message \ "state" )) + "," + compact(render( message \ "county" )), 1) )
    val reduced_message_counts_by_county = message_counts_by_county.reduceByKey( _ + _ )
    val tup_message_counts_by_county = reduced_message_counts_by_county.map( tup => {val state_county = tup._1.split(",")
 								        (state_county(0).tail.dropRight(1), state_county(1).tail.dropRight(1), tup._2)})
<<<<<<< HEAD

    tup_messages_by_county.saveToCassandra("puppy", "by_county_rt", SomeColumns("state", "county", "count") )
      
=======
    
    tup_message_counts_by_county.saveToCassandra("puppy", "by_county_rt", SomeColumns("state", "county", "count") )


    val messages_by_county = parsed_message.map( message => (compact(render( message \ "state" )) + "," + compact(render( message \ "county" )), message) )
    val reduced_messages_by_county = messages_by_county.reduceByKey( _ + _ )
    val tup_messages_by_county = reduced_messages_by_county.map( tup => {val state_county = tup._1.split(",")
                                                                        (state_county(0).tail.dropRight(1), state_county(1).tail.dropRight(1), tup._2)})
    
    tup_messages_by_county.saveToCassandra("puppy", "by_county_rt_msgs", SomeColumns("state", "county", "messages") )


>>>>>>> c132f50555d3731d204a3a9ae0ff62e990b94d80
    ssc.start()
    ssc.awaitTermination()
  }

}
