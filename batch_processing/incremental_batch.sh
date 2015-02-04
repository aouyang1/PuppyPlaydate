FILE_LIST=$(hdfs dfs -ls /user/PuppyPlaydate/cached | awk '{printf $8","}')
FILE_LIST=${FILE_LIST:0:-1}

spark-submit --class "batch_msgcnt" --master local[2] --jars target/scala-2.10/uber-assembly.jar target/scala-2.10/countmessagebycounty_2.10-1.0.jar $FILE_LIST

FILE_LIST=${FILE_LIST//,/ }
sudo -u hdfs hdfs dfs -rm $FILE_LIST
