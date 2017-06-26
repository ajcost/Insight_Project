#!/bin/bash
# This script pulls all submission and comment data from the parquet compression
# then runs the batch processes to compute the graph structures and most user comments

# Files in array

 while read F ;
 do
 echo $F

 year="${F:1:4}"
 month="${F:6:7}"
 
 spark-submit --class edu.upenn.ajcost.SparkBatchProcess.App --master spark://ip-10-0-0-9:7077 --master spark://ip-10-0-0-9:7077 --executor-memory 14G --driver-memory 14G SparkJsonParser-0.0.1-SNAPSHOT-jar-with-dependencies.jar "hdfs://ip-10-0-0-9:9000/reddit_data/$year/RS$F.parquet" "hdfs://ip-10-0-0-9:9000/reddit_data/$year/RC$F.parquet" "hdfs://ip-10-0-0-10:9000/reddit_edgelist/$year/edgelist_$year-$month" "$year_$month"

 echo "Stored in Cassandra"
 hdfs dfs -ls /reddit_edgelist/

done <filenames.txt