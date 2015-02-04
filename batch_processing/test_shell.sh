
FILE_LIST=$(hdfs dfs -ls /user/PuppyPlaydate/history | awk '{printf $8","}')
FILE_LIST=${FILE_LIST:0:-1}
echo $FILE_LIST
