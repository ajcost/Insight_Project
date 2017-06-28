package edu.upenn.ajcost.SparkGraphComputation;

/*****************************
*
* @author adamcostarino
*
* Description : Computes the PageRank of users from an input file
*               in edgelist format. The page ranks from each month
*               are averaged over the entire year. Null values are 
*               not registered as 0 and therefore do not affect the
*               ultimate PageRank calculation.
*
* File Structure:
* [username]  [neighbor username]
*
******************************/

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Iterables;

import scala.Tuple2;
import scala.collection.JavaConversions;


public final class App {
  private static final Pattern SPACE = Pattern.compile("\\s+");

  /**
  *
  * Description : Static method that takes two Doubles and returns the sum
  *
  * @a Double : Value to be added
  * @b Double : Value to be added
  * @return Double : the sum of the inputs
  *
  **/
  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }
  
  /**
  *
  * Description : Peforms PageRank algorithm for a specified number of iterations
  *
  * @filename String : the path to the input edgelist file in String representation
  * @spark SparkSession : current SparkSession instance
  * @repetitions Integer : the number of time the PageRank algorithm is to repeat
  * @monthString String : represents what the column will be called
  *
  **/
  public static Dataset<Row> calculatePageRank(String filename, SparkSession spark, int totalRepetitions, String monthString) {

    // Load edgelist file
  	JavaRDD<String> lines = spark.read().textFile(filename).javaRDD();
  	JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
  		String[] parts = SPACE.split(s);
     return new Tuple2<>(parts[0], parts[1]);
   }).groupByKey();

    // Initialize unitary PageRank
    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

    // Calculate contributions
    for (int rep = 0; rep < totalRepetitions; rep++) {
     JavaPairRDD<String, Double> contributions = links.join(ranks).values().flatMapToPair(s -> {
      int usersCount = Iterables.size(s._1());
      List<Tuple2<String, Double>> results = new ArrayList<>();
      for (String n : s._1) {
       results.add(new Tuple2<>(n, s._2() / usersCount));
     }
     return results.iterator();
   });
			// Re-calculates ranks based on adacent node contributions.
     ranks = contributions.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
     
   }
   
		// Programatically specify the schema
   List<StructField> fields = new ArrayList<>();
   StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
   fields.add(nameField);
   StructField pagerankField = DataTypes.createStructField("month_" + monthString, DataTypes.DoubleType, true);
   fields.add(pagerankField);
   StructType schema = DataTypes.createStructType(fields);
   
   
   JavaRDD<Row> rowRanksRDD = ranks.map(tuple -> RowFactory.create(tuple._1, tuple._2));
   Dataset<Row> ranksDF = spark.sqlContext().createDataFrame(rowRanksRDD, schema);
   return ranksDF;	
 }
 
  /**
  *
  * Description : Main
  *
  * @args[0]-args[11] Strings : the paths to the input edgelist files in String representation
  * @args[12] String : the year of the PageRank calculation
  * @args[13] String : the path to the output file in String representation -> parquet file format
  *
  **/
  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
    .builder()
    .appName("JavaPageRank")
    .getOrCreate();
    
    Dataset<Row> pageranks = calculatePageRank(args[0], spark, 20, "0");
    String[] columnjoins = new String[]  { "name" };

    // Iteratively calculate PageRanks for all twelve months in a single year
    for (int i = 1; i < 12; i++) {
      Dataset<Row> pageranksAdd = calculatePageRank(args[i], spark, 20, i + "");
      pageranks = pageranks.join(pageranksAdd,
       JavaConversions.asScalaBuffer(new ArrayList<String>(Arrays.asList(columnjoins))), "outer");
      pageranks.show();
    }

    // Specify the new Row encoding
    StructType structType = new StructType();
    structType = structType.add("name", DataTypes.StringType, true);
    structType = structType.add("average_" + args[12], DataTypes.DoubleType, true);
    ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
    
    // Map down the dataset by averaging the PageRanks for each user -> pseudo-iterative calculation
    pageranks = pageranks.map(
      new MapFunction<Row, Row>() {
        @Override
        public Row call(Row value) throws Exception {
         int monthCount = value.size() - 1;
         int nullCount = 0;
         double sum = 0;
         for (int i = 0; i < 12; i++) {
          if (value.isNullAt(i + 1)) {
           nullCount++;
         } else {
           sum += value.getDouble(i + 1);
         }
       }
       Row newRow = RowFactory.create(value.getString(0), sum/(monthCount - nullCount));
       return newRow;
     }
   }, encoder);
    pageranks.show();
    pageranks.write().parquet(args[13]);
    
  }
}
