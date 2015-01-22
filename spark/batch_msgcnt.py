from pyspark import SparkContext
import json

if __name__ == "__main__":
    sc = SparkContext(appName="MessageCountByCountyAndTime")
    jsonRDD = sc.textFile("/user/PuppyPlaydate/historical/random_messages_0.txt")
    total_count_RDD = jsonRDD.map(lambda s: (json.loads(s)['county'], 1)).reduceByKey(lambda a, b: a + b)
    total_count = total_count_RDD.collect()
    for (county, count) in total_count:
        print "%s: %d" % (county, count)

    sc.stop()

    
