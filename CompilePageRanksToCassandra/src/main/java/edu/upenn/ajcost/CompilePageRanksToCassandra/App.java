package edu.upenn.ajcost.CompilePageRanksToCassandra;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.cassandra.*;

import scala.collection.JavaConversions;

public class App {
	
    public static void main( String[] args ) {
        SparkSession spark = SparkSession
        	      .builder()
        	      .appName("PageRanksToCassandra")
        	      .getOrCreate();
        
        Dataset<Row> firstyear = spark.read().parquet(args[0]).persist(StorageLevel.MEMORY_AND_DISK());
        
        String[] columnjoins = new String[]  { "name" };
        
        for (int i = 1; i < 10; i++) {
            Dataset<Row> yearAdd = spark.read().parquet(args[i]).persist(StorageLevel.MEMORY_AND_DISK());
        	firstyear = firstyear.join(yearAdd,
        			JavaConversions.asScalaBuffer(new ArrayList<String>(Arrays.asList(columnjoins))), "outer");
            firstyear.show();
        }
        firstyear.write().format("org.apache.spark.sql.cassandra").option("table", "allranks").option("keyspace", "pageranks").save();
    }
}
