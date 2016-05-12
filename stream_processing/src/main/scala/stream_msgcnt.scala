import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql._
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

    val conf = new SparkConf().setAppName("stream_example")
	.set("spark.cassandra.connection.host", "ec2-52-24-202-29.us-west-2.compute.amazonaws.com")
    val ssc = new StreamingContext(conf, Seconds(5))
       
    val zkQuorum = "52.39.138.152:2181,52.36.139.122:2181,52.34.29.103:2181"
    val groupID = "rt"
    val topics = Map("messages" -> 100)

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupID, topics).map(_._2)

    messages.foreachRDD { rdd =>
	val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val df = sqlContext.jsonRDD(rdd)
	df.registerTempTable("msgs")
	
        sqlContext.sql("SELECT state, county, CAST(COUNT(message) as INT) as count FROM msgs GROUP BY state, county")
            .map{ case Row(state: String, county: String, count: Int) => MessageByCounty(state, county, count) }
            .saveToCassandra("puppy","by_county_rt")
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

case class MessageByCounty(state: String, county: String, count: Int)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
