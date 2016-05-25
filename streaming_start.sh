#!/bin/sh

SESSION_NAME="spark_streaming"

tmux new-session -s ${SESSION_NAME} -n bash -d
tmux send-keys -t ${SESSION_NAME} 'spark-submit --class StreamExample --master spark://ip-172-31-42-205:7077 --executor-memory 14500M --driver-memory 14500M --jars stream_processing/target/scala-2.10/stream_example-assembly-1.0.jar stream_processing/target/scala-2.10/stream_example_2.10-1.0.jar' C-m


