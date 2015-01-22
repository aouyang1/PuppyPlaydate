import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import com.datastax.spark.connector._

object batch_msgcnt {
  def main(args: Array[String]) {
    val textFile = "/user/PuppyPlaydate/historical/random_messages_0.txt"
    val conf = new SparkConf().setAppName("CountMessageByCounty").set("spark.cassandra.connection.host", "54.215.184.69")
    val sc = new SparkContext(conf)
    val jsonRDD = sc.textFile(textFile)
    val parsedRDD = jsonRDD.map( x => parse(x) )

    // all time historical count by county
    val mapped_county_full = parsedRDD.map( x => (compact(render(x \ "county")).tail.dropRight(1) , 1) )
    val county_full_cnt = mapped_county_full.reduceByKey(_+_)
    county_full_cnt.saveAsTextFile("/user/PuppyPlaydate/batchviews/test/by_county_full")

    // historical count by county and month
    val mapped_county_month = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                    compact(render(x \ "county")).tail.dropRight(1) + "," + 
                                                    date_array(0) + "," + 
                                                    date_array(1)} , 1) )
    val county_month_cnt = mapped_county_month.reduceByKey(_+_)    
    county_month_cnt.saveAsTextFile("/user/PuppyPlaydate/batchviews/test/by_county_month")

    // historical count by county and day
    val mapped_county_day = parsedRDD.map( x=> ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                 compact(render(x \ "county")).tail.dropRight(1) + "," +
                                                 date_array(0) + "," + 
                                                 date_array(1) + "," +
                                                 date_array(2)}, 1) )
    val county_day_cnt = mapped_county_day.reduceByKey(_+_)
    county_day_cnt.saveAsTextFile("/user/PuppyPlaydate/batchviews/test/by_county_day")
    
    // historical count by county and hour
    val mapped_county_hour = parsedRDD.map( x=> ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                 compact(render(x \ "county")).tail.dropRight(1) + "," + 
                                                 date_array(0) + "," +
                                                 date_array(1) + "," + 
                                                 date_array(2) + "," + 
                                                 date_array(3)}, 1) )
    val county_hour_cnt = mapped_county_hour.reduceByKey(_+_)
    county_hour_cnt.saveAsTextFile("/user/PuppyPlaydate/batchviews/test/by_county_hour")

    // calculate the county_week_cnt
    // write batch views into cassandra

    //county_cnt.saveToCassandra("test","kv",SomeColumns("county","count"))
}

}
