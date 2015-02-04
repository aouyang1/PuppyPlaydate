import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import com.datastax.spark.connector._
import scala.collection.JavaConversions._


object incremental_batch_msgcnt {
  def main(args: Array[String]) {

    def single_to_double(digit: String): String = if (digit.length==1) "0"+digit else digit

    val conf = new SparkConf().setAppName("IncrementBatch").set("spark.cassandra.connection.host", "54.215.184.69")
    val sc = new SparkContext(conf)

    val jsonRDD = sc.textFile(args(0))
 
    // parse each record as a json
    val parsedRDD = jsonRDD.map( x => parse(x) ).cache()
  
     
    //-------- MAP TO KEY, VALUE --------//

    // historical count by county and month
    //            ("state,county,year,month", 1)   
    // output ex: ("TX,Dallas County,2014,5", 1)
    val mapped_county_month = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                    compact(render(x \ "state")) + "," +
 	  				            compact(render(x \ "county")) + "," + 
                                                    date_array(0) + "," + 
                                                    date_array(1)} , 1) )
       
        
    // historical count by county and day
    //           ("state,county,year,month,day", 1) 
    // output ex: ("TX,Dallas County,2014,5,16", 1)
    val mapped_county_day = parsedRDD.map( x => ({val date_array = compact(x \ "timestamp").tail.dropRight(1).split(",")
                                                  compact(render(x \ "state")) + "," +
	      					  compact(render(x \ "county")) + "," +
                                                  date_array(0) + "," + 
                                                  date_array(1) + "," +
                                                  date_array(2)}, 1) )
       
   
    
    //-------- REDUCE AND SAVE TO CASSANDRA --------//

    // reduce and map county_month to tuple for cassandra
    val county_month_cnt = mapped_county_month.reduceByKey(_+_)
    val county_month_cnt_tup = county_month_cnt.map( tup =>  {val keys = tup._1.split(",")
                                                             (keys(0).tail.dropRight(1),
                                                              keys(1).tail.dropRight(1),
                                                             (keys(2)+single_to_double(keys(3))).toInt,
                                                              tup._2)} )
    county_month_cnt_tup.saveToCassandra("puppy", "by_county_month", SomeColumns("state", "county", "date", "count")) 
    

    // reduce and map county_day to tuple for cassandra
    val county_day_cnt = mapped_county_day.reduceByKey(_+_)
    val county_day_cnt_tup = county_day_cnt.map( tup =>  {val keys = tup._1.split(",")
                                                         (keys(0).tail.dropRight(1),
                                                          keys(1).tail.dropRight(1),
                                                         (keys(2)+single_to_double(keys(3))+single_to_double(keys(4))).toInt,
                                                          tup._2)} )
    county_day_cnt_tup.saveToCassandra("puppy", "by_county_day", SomeColumns("state", "county", "date", "count")) 
 
  }

}
