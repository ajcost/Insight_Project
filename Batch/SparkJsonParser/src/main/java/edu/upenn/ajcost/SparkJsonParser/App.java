package edu.upenn.ajcost.SparkJsonParser;

import java.util.function.Function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.Map;

/*******************************************
 * @author
 * 
 * 
 ******************************************/


public class App {

	public static void writeToCassandra(CassandraConnector connector, String tableName, 
			JavaPairRDD<String, Map<String, Long>> adjacencies) {
		
		// Convert to Java Map
    	JavaPairRDD<String, MapStringLong> javaAdj = adjacencies.mapToPair(
    			new PairFunction<Tuple2<String,Map<String,Long>>, String, MapStringLong>() {
					public Tuple2<String, MapStringLong> call(Tuple2<String, Map<String, Long>> t) {
						return new Tuple2(t._1, JavaConversions.mutableMapAsJavaMap(t._2));
					}		
    			});
    	
		try (Session session = connector.openSession()) {
			session.execute("CREATE TABLE reddit." + tableName + "(name TEXT PRIMARY KEY, adj map<TEXT, BIGINT> )");
		}
		
		CassandraJavaUtil.javaFunctions(javaAdj).writerBuilder("reddit", tableName, 
				CassandraJavaUtil.mapTupleToRow(String.class, MapStringLong.class)).saveToCassandra();
	}

    public static void main(String[] args) {
    	// Configure and Start Spark Session
    	SparkSession spark = SparkSession
    			.builder()
       			.appName("DataAnalytics")
       			.config("spark.cassandra.connection.host", "34.225.57.57")
    			.getOrCreate();
    	
    	String submissionPath = args[0];
    	String commentPath = args[1];
    	String edgeListPath = args[2];
    	String tableName = args[3] + "_" + args[4];
    	
    	Dataset<Row> postList =  spark.read().parquet(submissionPath);
    	Dataset<Row> commentList = spark.read().parquet(commentPath);    	
    	
    	//Functions.createUserEdgeList(commentList, postList, edgeListPath);
    	
    	Dataset<Row> all = postList.drop("id").union(commentList.drop("id"));
    	
    	JavaPairRDD<String, Map<String, Long>> subredditAdjacencies = Functions.createSubredditAdjacencies(all, spark);
    	JavaPairRDD<String, Map<String, Long>> userAdjacencies = Functions.getUserSubredditPosts(all, spark);
        	
    	CassandraConnector connector = CassandraConnector.apply(spark.sparkContext().getConf());
    	
    	writeToCassandra(connector, "subs_" + tableName, subredditAdjacencies);
    	writeToCassandra(connector, "users_" + tableName, userAdjacencies);
    	
    }
}
