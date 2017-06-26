package edu.upenn.ajcost.SparkBatchProcess;

/*****************************
*
* @author adamcostarino
*
* Description : Performs large Spark Batch Process on the parquet files
*               stored on the HDFS. Creates Edgelist between users, subreddit
*               adjacency list, and the user subreddit preferential list.
*
* Input File Structure (parquet) :
* [username] [subreddit] [link id number]
*
******************************/

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

public class App {
    /**
    *
    * Description : Writes JavaPair Rdd to Cassandra
    *
    * @connector CassandraConnector : the connector to the casssandra cluster
    * @tableName String : name of the table that is going to be written to in String representation
    * @adjacencies JavaPairRDD<String, Map<String, Long>> : adjacency list represented as JavaPairRDD
    *
    **/
    public static void writeToCassandra(CassandraConnector connector, String tableName, 
        JavaPairRDD<String, Map<String, Long>> adjacencies) {

        // Convert to Java Map
        JavaPairRDD<String, MapStringLong> javaAdj = adjacencies.mapToPair(
            new PairFunction<Tuple2<String,Map<String,Long>>, String, MapStringLong>() {
                public Tuple2<String, MapStringLong> call(Tuple2<String, Map<String, Long>> t) {
                    return new Tuple2(t._1, JavaConversions.mutableMapAsJavaMap(t._2));
                }		
            });

        // Create new table in Cassandra then write
        try (Session session = connector.openSession()) {
            session.execute("CREATE TABLE reddit." + tableName + "(name TEXT PRIMARY KEY, adj map<TEXT, BIGINT> )");
        }
        CassandraJavaUtil.javaFunctions(javaAdj).writerBuilder("reddit", tableName, 
            CassandraJavaUtil.mapTupleToRow(String.class, MapStringLong.class)).saveToCassandra();
    }

    /**
    *
    * Description : Main
    *
    * @args[0] String : the path to submission parquet file in String format
    * @args[1] String : the path to comment parquet file in String format
    * @args[2] String : the path to output edge list text file in String format
    * @args[3]-@args[4] String : the table name to be written to in Cassandra in String format
    * @args[5] String : IP address of one of the Cassandra worked nodes in String format
    *
    **/
    public static void main(String[] args) {
        // Configure and Start Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("DataAnalytics")
                .config("spark.cassandra.connection.host", args[5])
                .getOrCreate();
        
        String submissionPath = args[0];
        String commentPath = args[1];
        String edgeListPath = args[2];
        String tableName = args[3] + "_" + args[4];

        Dataset<Row> postList =  spark.read().parquet(submissionPath);
        Dataset<Row> commentList = spark.read().parquet(commentPath);

        Functions.createUserEdgeList(commentList, postList, edgeListPath);

        Dataset<Row> all = postList.drop("id").union(commentList.drop("id"));
        JavaPairRDD<String, Map<String, Long>> subredditAdjacencies = Functions.createSubredditAdjacencies(all, spark);
        JavaPairRDD<String, Map<String, Long>> userAdjacencies = Functions.getUserSubredditPosts(all, spark);

        // Write both subreddit adjacency list and user subreddit preference list
        CassandraConnector connector = CassandraConnector.apply(spark.sparkContext().getConf());
        writeToCassandra(connector, "subs_" + tableName, subredditAdjacencies);
        writeToCassandra(connector, "users_" + tableName, userAdjacencies);
    }
}
