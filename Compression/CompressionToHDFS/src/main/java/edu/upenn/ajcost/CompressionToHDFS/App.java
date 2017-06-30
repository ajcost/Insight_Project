package edu.upenn.ajcost.CompressionToHDFS;

import org.apache.spark.sql.Column;

/*********************
 * @author adamcostarino
 * 
 * Description : Reads data from S3 and compresses to parquet file format on the HDFS
 * 
 *********************/

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

public class App {
    
    /**
     * 
     * @param spark : The current spark session that is running
     * @param submissionFilePath : String that represents the path to the file on S3
     * @param input : String that was input on the command line
     * @param locationPrefix : The HDFS location
     * 
     **/
    public static void writePoststoHDFS(SparkSession spark, Dataset<Row> listData, String input, String locationPrefix) {
       Dataset<Row> list = listData.toDF();
       list = list.toDF().filter(functions.col("author").notEqual("[deleted]"));
       list.write().parquet(locationPrefix + input + ".parquet");
       list = null;
   }
   
   public static void main(String[] args) {
      
        // Configure and Start Spark Session
       SparkSession spark = SparkSession
       .builder()
       .appName("CompressionToHDFS")
       .getOrCreate();
       
       String submissionFilePath = "s3a://ac-reddit-data/Raw/" + args[0];
       String commentFilePath = "s3a://ac-reddit-data/Raw/" + args[1];
       
       Dataset<Row> postList = spark.read().json(submissionFilePath).persist(StorageLevel.MEMORY_AND_DISK());
       Dataset<Row> postListData = postList.toDF()
       .select(new Column("author"),
           new Column("subreddit"),
           new Column("id"));
       
       writePoststoHDFS(spark, postListData, args[0], args[2]);
       
       Dataset<Row> commentList = spark.read().json(commentFilePath).persist(StorageLevel.MEMORY_AND_DISK());
       Dataset<Row> commentListData = commentList.toDF()
       .select(new Column("author"),
           new Column("subreddit"),
           functions.regexp_replace(commentList.col("link_id"), "t3_", "").as("id"));
       
       writePoststoHDFS(spark, commentListData, args[1], args[2]);
       
   }
}