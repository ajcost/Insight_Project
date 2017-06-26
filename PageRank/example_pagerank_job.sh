spark-submit --class edu.upenn.ajcost.SparkGraphComputation.App --executor-memory 14G --driver-memory 5G --master spark://ip-10-0-0-8:7077 SparkGraphComputation-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-01 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-02 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-03 \ 
 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-04 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-05 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-06 \ 
 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-07 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-08 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-09 \ 
 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-10 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-11 hdfs://ip-10-0-0-8:9000/reddit_edgelist/2015/edgelist_2015-12 \ 
 2015 hdfs://ip-10-0-0-8:9000/reddit_pageranks/2015.parquet
wait