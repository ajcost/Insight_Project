package edu.upenn.ajcost.SparkGraphComputation;

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

/**
 * Computes the PageRank of usernames from an input file:
 * 
 * [username]  [neighbor username]
 * 
 * where strings are separated by spaces.
 * 
 */
public final class App {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }
  
  public static Dataset<Row> calculatePageRank(String filename, SparkSession spark, int repetitions, String monthString) {
		JavaRDD<String> lines = spark.read().textFile(filename).javaRDD();

		// Loads all URLs from input file and initialize their neighbors.
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
			String[] parts = SPACES.split(s);
			return new Tuple2<>(parts[0], parts[1]);
		}).distinct().groupByKey().cache();

		// Loads all URLs with other URL(s) link to from input file and
		// initialize ranks of them to one.
		JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

		for (int current = 0; current < repetitions; current++) {
			// Calculates URL contributions to the rank of other URLs.
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(s -> {
				int usersCount = Iterables.size(s._1());
				List<Tuple2<String, Double>> results = new ArrayList<>();
				for (String n : s._1) {
					results.add(new Tuple2<>(n, s._2() / usersCount));
				}
				return results.iterator();
			});

			// Re-calculates URL ranks based on neighbor contributions.
			ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
			
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
  
  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaPageRank")
      .getOrCreate();
    
    
    Dataset<Row> pageranks = calculatePageRank(args[0], spark, 20, "0");
    
    String[] columnjoins = new String[]  { "name" };
    
    for (int i = 1; i < 12; i++) {
        Dataset<Row> pageranksAdd = calculatePageRank(args[i], spark, 20, i + "");
    	pageranks = pageranks.join(pageranksAdd,
    			JavaConversions.asScalaBuffer(new ArrayList<String>(Arrays.asList(columnjoins))), "outer");
        pageranks.show();
    }
    
    StructType structType = new StructType();
    structType = structType.add("name", DataTypes.StringType, true);
    structType = structType.add("average_" + args[12], DataTypes.DoubleType, true);

    ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
    
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
