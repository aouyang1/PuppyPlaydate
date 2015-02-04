#!/bin/sh

FILE_LIST=$(hdfs dfs -ls /user/PuppyPlaydate/cached | awk '$8 !~ /_COPYING_/ {printf $8","}')
FILE_LIST=${FILE_LIST:1:-1}
echo $FILE_LIST

spark-submit --class "incremental_batch_msgcnt" --master local[2] --jars incr_batch_processing/target/scala-2.10/uber-assembly.jar incr_batch_processing/target/scala-2.10/incrementbatch_2.10-1.0.jar $FILE_LIST

FILE_LIST=${FILE_LIST//,/ }
sudo -u hdfs hdfs dfs -rm $FILE_LIST

