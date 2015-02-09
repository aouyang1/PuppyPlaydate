# Puppy Playdate
======================================

## Real-time and historical analytics on pet walking activity for pet owners
[www.puppyplaydate.website](http://www.puppyplaydate.website)

Puppy Playdate is a tool to help pet owners track pet activity in various counties across the United States providing both real-time and historical updates on the most recent message activity levels using the following technologies:
- Apache Kafka 0.8.2.0
- Apache HDFS
- Spark
- Spark Streaming
- Apache Cassandra
- Flask with Highcharts, Bootstrap and Ajax

# What Puppy Playdate Provides
Puppy Playdate permits users to dive into real-time and historical analytics on the activity of pet owners based on sent messages. The user has the ability to look at current activity on the US map and the latest messages in the messaging box. The interface also provides historical analytics on the number of messages sent within each US county on a monthly and daily basis. 

![Puppy Playdate Demo] (images/dashboard.png)

# Puppy Playdate Approach
Puppy Playdate uses synthetically generated JSON messages which are processed in the batch and real-time component for historical monthly and daily message counts along with continual map and message updates.

![Puppy Playdate Demo] (images/pipeline.png)

## Data Synethesis
Current list of counties in the United States were extracted from http://www.bls.gov/lau/laucntycur14.txt. Area Title contained both the county name and state. The Civilian Labor Force field was used to scale the frequency messages would be sent to each county where higher labor force related to a higher frequency of messages to be sent. Time based trends were also implemented by altering the probability of a message being sent a specific year, month and day of week. Synthesized trends are as follows:
- Increasing growth in messages by each year 2012, 2013, 2014
- More messages sent in April and October with October being the peak activity
- Weekly patterns with peak activity on Saturdays

JSON message fields:
- timestamp [year month day hour minute second]: time when message was posted
- county [county, state]: location where message was posted
- creatorID: Integer representing the creator of the message thread
- senderID: Integer representing the sender of the current message
- rank: Integer representing the order of the message in the thread with 0 being the first message
- messageID: Integer representing a specific message thread
- message: String containing the message

~ 45 GB of historical data
~ 1000 messages streaming per second

## Data Ingestion
JSON messages were produced and consumed by python scripts using the kafka-python package from https://github.com/mumrah/kafka-python.git. Messages were published to a single topic with Spark Streaming and HDFS acting as consumers. Messages were blocked into 20MB sizes into the main historical folder and cached folder on HDFS. The incremental batch job operates continually on the cached folder and removes processed files while leaving the historical folder immutable for a complete rebuild of the batch view.

## Batch Processing
Two batch processes were performed for historical batch views:

1. Count number of messages by county on a monthly granularity
2. Count number of messages by county on a daily granularity

Batch views were directly written into cassandra with the spark-cassandra connector

sbt libarary dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1"
- "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

A full batch process was made in the case of needing to rebuild the entire batch view. Typically the incremental batch process is run daily from the cached folder on HDFS. 

## Real-time Processing
Two stream processes were performed for real-time views:

1. Count number of messages by county on a 5 second interval
2. Retrieve the 5 most recent messages in a county

Messages streamed into Spark Streaming with the spark-kafka connector
Real-time views were directly written into cassandra with the spark-cassandra connector

sbt library dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1"
- "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
- "org.apache.spark" % "spark-streaming_2.10" % "1.2.0" % "provided"
- "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0"
  
## Cassandra Schema
Tables:

1. by_county_rt: table populated by Spark Streaming (real-time) representing message count in the last 5 seconds
2. by_county_msgs: table populated by Spark Streaming (real-time) containing recent messages organized by county and date
3. by_county_month: table populated by Spark (batch) containing the historical message counts on a monthly basis
4. by_county_day: table populated by Spark (batch) containing the historical message counts on a daily basis
```
CREATE TABLE by_county_rt (state varchar, county varchar, count int, PRIMARY KEY ( (state, county) ) );
CREATE TABLE by_county_msgs (state varchar, county varchar, date int, time int, message varchar, PRIMARY KEY ( (state, county), date, time ) );
CREATE TABLE by_county_month (state varchar, county varchar, date int, count, int, PRIMARY KEY ( (state, county), date ) );
CREATE TABLE by_county_day (state varchar, county varchar, date int, count, int, PRIMARY KEY ( (state, county), date ) );
```
Date/Time format: 
- by_county_msgs: 
  - yyyymmdd (ex: 20150209)
  - HHMMSS (ex: 120539)
- by_county_month: yyyymm (ex: 201502)
- by_county_day: yyyymmdd (ex: 20150209)

## API calls
Data in JSON format can be displayed in the browser by calling the following from the root index puppyplaydate.website:

- /update_map/
  - retrieve number of messages in the last 5 seconds for all counties in the US
- /new_messages/county_code/
  - retrieve 5 most recent messages from a county
  - county_code example: puppyplaydate.website/new_messages/us-ca-081/
- /update_chart/interval/county_code/
  - return a time series requested by the time interval and county_code
  - interval options: month or day
  - example: puppyplaydate.website/month/us-ca-081/

## Startup Protocol
1. Kafka server
2. Spark Streaming
3. HDFS Kafka consumer
4. Kafka message producer
