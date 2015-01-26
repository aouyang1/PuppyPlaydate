name := "stream_example"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1",
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0"
)

