#!/bin/bash
# This script pulls all submission and comment data from the S3 Bucket
# then compresses them to the HDFS with parquet

# Files in array

 while read F ;
 do
 echo $F

 year="${F:1:4}"
 
 spark-submit --class edu.upenn.ajcost.CompressionToHDFS.App --master spark://ip-10-0-0-9:7077 --executor-memory 14G --driver-memory 14G CompressiontoHDFS-0.0.1-SNAPSHOT-jar-with-dependencies.jar "RS$F" "RC$F" "hdfs://ip-10-0-0-9:9000/reddit_data/$year/"

 echo "Stored in HDFS "
 hadoop fs -ls /reddit_data/

done <filenames.txt
