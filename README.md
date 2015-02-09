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
- 2x growth in messages by each year 2012, 2013, 2014
- More messages sent in April and October with October being the peak activity
- Weekly patterns with peak activity on Saturdays

## Data Ingestion

## Batch Processing

## Real-time Processing
