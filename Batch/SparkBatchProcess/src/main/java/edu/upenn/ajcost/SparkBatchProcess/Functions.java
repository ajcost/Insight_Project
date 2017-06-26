package edu.upenn.ajcost.SparkBatchProcess;

/*****************************
 * 
 * @author adamcostarino
 *
 * Description : These static functions build the edge list files,
 *               the subreddit adjacencies, and the user subreddit posts.
 *
 ******************************/

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.collection.mutable.Map;


public class Functions {

	/**
	 * 
	 * @param commentList : Dataset of all the comments made
	 * @param postList : Dataset of all the posts made
	 * @param destination : String of where to save the RDD
	 *
	 **/
	public static void createUserEdgeList(Dataset<Row> commentList, Dataset<Row> postList, String destination) {
		Dataset<Row> edgeList = commentList.dropDuplicates();
		edgeList = edgeList.join(postList, "id");
		edgeList = edgeList.drop("subreddit").drop("id");
		
		@SuppressWarnings("serial")
		Dataset<String> edges = edgeList.map(
			new MapFunction<Row, String>() {
				public String call(Row row) throws Exception {
					return row.getString(0) + " " + row.getString(1);
				}
			}, Encoders.STRING());
		edges.toJavaRDD().saveAsTextFile(destination);
	}

	/**
	 * 
	 * @param all : Dataset representing the entire 
	 * @param  spark : the current spark session
	 * @return finalAdjacencies : the adjacency list for each subreddit -> (subreddit_name,[(adj_subreddit_name_1,3) , (adj_subreddit_name_1) . . . ]))
	 * 
	 **/
	public static JavaPairRDD<String, Map<String, Long>> createSubredditAdjacencies(Dataset<Row> all, SparkSession spark) {
		all.toDF().createOrReplaceTempView("allTable");
		Dataset<Row> subredditInterconnections = spark.sql("SELECT a.author as author, "
		                                                    + "a.subreddit as subredditOne, "
		                                                    + "b.subreddit as subredditTwo "
	                                                        + "FROM allTable as a INNER JOIN allTable as b "
    	                                                    + "ON a.author = b.author AND a.subreddit < b.subreddit");

		subredditInterconnections = subredditInterconnections.drop("author");
    	JavaPairRDD<Tuple2<String, String>, Long> mappedInterconnections = 
    			subredditInterconnections.select("subredditOne", "subredditTwo").toJavaRDD().mapToPair(input ->
    				new Tuple2<Tuple2<String, String>, Long>(
    					(new Tuple2<String, String>(input.getString(0), input.getString(1))), (long) 1));
    	
    	// Count intersections then union for all permutations
    	mappedInterconnections = mappedInterconnections.reduceByKey((a, b) -> a + b);
    	JavaPairRDD<Tuple2<String, String>, Long> mappedInterconnectionsTwo = mappedInterconnections.mapToPair(input ->
    			new Tuple2<Tuple2<String, String>, Long>(new Tuple2<String, String>(input._1._2, input._1._1), input._2));
    	JavaPairRDD<Tuple2<String, String>, Long> allAdjacencies = mappedInterconnections.union(mappedInterconnectionsTwo);
    	
    	
    	// Insert List as second element in Pair RDD
		@SuppressWarnings("serial")
		JavaPairRDD<String, Map<String, Long>> newAdjacencies = allAdjacencies.mapToPair(
				new PairFunction<Tuple2<Tuple2<String, String>,Long>, String, Map<String, Long>>() {
					public Tuple2<String, Map<String, Long>> call(Tuple2<Tuple2<String, String>,Long> val) {
						Map<String, Long> temp = scala.collection.mutable.Map$.MODULE$.<String, Long>empty();
						temp.put(val._1._2, val._2);
						return new Tuple2<String, Map<String, Long>>(val._1._1, temp);
					}
				});
		
		// Add all lists together to get the full adjacency list for each subreddit
    	@SuppressWarnings("serial")
		JavaPairRDD<String, Map<String, Long>> finalAdjacencies = newAdjacencies.reduceByKey(
    			new Function2<Map<String, Long>, Map<String, Long>, Map<String, Long>>() {
    				@Override
    				public Map<String, Long> call(Map<String, Long> one , Map<String, Long> two) {
    					one.$plus$plus$eq(two);
    					return one;
            }
        });
		
    	return finalAdjacencies;
	}
	
	/**
	 * 
	 * @param all : Dataset of the total data set of user posts
	 * @param spark : the current spark session
	 * @return finalTable : the user mapped to an adjacency list representing where he posts
	 * 
	 **/
	public static JavaPairRDD<String, Map<String, Long>> getUserSubredditPosts(Dataset<Row> all, SparkSession spark) {
	    JavaPairRDD<Tuple2<String, String>, Long> allMapped = 
		    all.select("author", "subreddit").toJavaRDD().mapToPair(input ->
			    new Tuple2<Tuple2<String, String>, Long>(
				    (new Tuple2<String, String>(input.getString(0), input.getString(1))), (long) 1));
		
	    allMapped = allMapped.reduceByKey((a, b) -> a + b);
		
	    @SuppressWarnings("serial")
	    JavaPairRDD<String, Map<String, Long>> newMapped = allMapped.mapToPair(
		    new PairFunction<Tuple2<Tuple2<String, String>,Long>, String, Map<String, Long>>() {
			    public Tuple2<String, Map<String, Long>> call(Tuple2<Tuple2<String, String>,Long> val) {
			        Map<String, Long> temp = scala.collection.mutable.Map$.MODULE$.<String, Long>empty();
			        temp.put(val._1._2, val._2);
			        return new Tuple2<String, Map<String, Long>>(val._1._1, temp);
		        }
		    });
	    
		// Add all Lists together to get the full adjacency list for user posts
    	@SuppressWarnings("serial")
		JavaPairRDD<String, Map<String, Long>> finalTable = newMapped.reduceByKey(
    			new Function2<Map<String, Long>, Map<String, Long>, Map<String, Long>>() {
    				@Override
    				public Map<String, Long> call(Map<String, Long> one , Map<String, Long> two) {
    					one.$plus$plus$eq(two);
    					return one;
            }
        });

		return finalTable;
	}
}