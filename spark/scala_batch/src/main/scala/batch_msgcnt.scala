import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import com.datastax.spark.connector._

object batch_msgcnt {
  def main(args: Array[String]) {
    val textFile1 = "/user/PuppyPlaydate/history/random_messages_0.txt"
    val textFile2 = "/user/PuppyPlaydate/history/random_messages_1.txt"

    val conf = new SparkConf().setAppName("CountMessageByCounty").set("spark.cassandra.connection.host", "54.215.184.69")
    val sc = new SparkContext(conf)

    val jsonRDD1 = sc.textFile(textFile1)
    val jsonRDD2 = sc.textFile(textFile2)
    val jsonRDD = sc.union(jsonRDD1, jsonRDD2)

    val parsedRDD = jsonRDD.map( x => parse(x) ).cache()
    
    def single_to_double(digit: String): String = if (digit.length==1) "0"+digit else digit

     
    // all time historical count by county
    val mapped_county_full = parsedRDD.map( x => (compact(render(x \ "state")) + "," + compact(render(x \ "county")) , 1) )
    val county_full_cnt = mapped_county_full.reduceByKey(_+_)
    val county_full_tup = county_full_cnt.map( tup => {val state_county_arr = tup._1.split(",")
                             				    (state_county_arr(0).tail.dropRight(1), state_county_arr(1).tail.dropRight(1), tup._2)} )
    county_full_tup.saveToCassandra("puppy", "by_county_full", SomeColumns("state", "county", "count"))
    



    // historical count by county and month
    val mapped_county_month = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                    compact(render(x \ "state")) + "," +
						                            compact(render(x \ "county")) + "," + 
                                                    date_array(0) + "," + 
                                                    date_array(1)} , 1) )
    val county_month_cnt = mapped_county_month.reduceByKey(_+_)    
    val county_month_cnt_tup = county_month_cnt.map( tup =>  {val keys = tup._1.split(",")
                                                             (keys(0).tail.dropRight(1), 
                            							      keys(1).tail.dropRight(1), 
                            							     (keys(2)+single_to_double(keys(3))).toInt, 
                            							      tup._2)} )
    county_month_cnt_tup.saveToCassandra("puppy", "by_county_month", SomeColumns("state", "county", "date", "count"))
   


        
    // historical count by county and day
    val mapped_county_day = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                 compact(render(x \ "state")) + "," +
						 compact(render(x \ "county")) + "," +
                                                 date_array(0) + "," + 
                                                 date_array(1) + "," +
                                                 date_array(2)}, 1) )
    val county_day_cnt = mapped_county_day.reduceByKey(_+_)
    val county_day_cnt_tup = county_day_cnt.map( tup =>  {val keys = tup._1.split(",")
                                                        (keys(0).tail.dropRight(1), 
							 keys(1).tail.dropRight(1), 
							(keys(2)+single_to_double(keys(3))+single_to_double(keys(4))).toInt, 
							 tup._2)} )
    county_day_cnt_tup.saveToCassandra("puppy", "by_county_day", SomeColumns("state", "county", "date", "count"))


    

    // historical count by county and hour
    val mapped_county_hour = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
						                         compact(render(x \ "state")) + "," +
                                                 compact(render(x \ "county")) + "," + 
                                                 date_array(0) + "," +
                                                 date_array(1) + "," + 
                                                 date_array(2) + "," + 
                                                 date_array(3)}, 1) )
    val county_hour_cnt = mapped_county_hour.reduceByKey(_+_)
    val county_hour_cnt_tup = county_hour_cnt.map( tup =>  {val keys = tup._1.split(",")
                                                           (keys(0).tail.dropRight(1), 
                                						    keys(1).tail.dropRight(1), 
                                						   (keys(2)+single_to_double(keys(3))+single_to_double(keys(4))+single_to_double(keys(5))).toInt, 
                                						    tup._2)} )

    county_hour_cnt_tup.saveToCassandra("puppy", "by_county_hour", SomeColumns("state", "county", "date", "count"))
    
  }

}
