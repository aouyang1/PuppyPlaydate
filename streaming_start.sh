#!/bin/sh

SESSION_NAME="spark_streaming"

tmux new-session -s ${SESSION_NAME} -n bash -d
tmux send-keys -t ${SESSION_NAME} 'sudo -u hdfs spark-submit --class StreamExample --master local[2] --jars stream_processing/target/scala-2.10/uber-assembly.jar stream_processing/target/scala-2.10/stream_example_2.10-1.0.jar' C-m


