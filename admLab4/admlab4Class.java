package admLab4;

import java.util.Scanner;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import java.util.Arrays;

public class admlab4Class {
	public static void main(String[] args) throws Exception {
		System.out.println("--------------------------------------------MENU-------------------------------------------------");
		System.out.println("1--Section A : The Program by using the low-level API for following request. Retrieve, for each movie, the number of users that rated it.");
		System.out.println("2--Section A : The Program by using the low-level API for following request. Retrieve all the user ratings of each movie directed by Martin Brest.");
		System.out.println("3--Section A : The Program by using the low-level API for following request. Retrieve all the movies produced in 2000 whose average user rating is greater than 2.5.");
		System.out.println("4--Section B : The Program by using the structured API for following request. Retrieve, for each movie, the number of users that rated it.");
		System.out.println("-------------------------------------------------------------------------------------------------");
		System.out.print("Please Enter Your Selection as Number : ");
	
		Scanner scanner = new Scanner(System.in);
		int choice = scanner.nextInt();
		
		String inputFile = "hdfs://master:9000/user/user35/" + args[0];
		String inputFile2 = "hdfs://master:9000/user/user35/" + args[1];
		String outputFile = "hdfs://master:9000/user/user35/" + args[2];

		SparkConf conf = new SparkConf().setAppName("Query2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(inputFile);
		JavaRDD<String> textFile2 = sc.textFile(inputFile2);
		
		switch (choice) {
		case 1:	 
			// Section A : The Program by using the low-level API for following request. Retrieve, for each movie, the number of users that rated it.
	        JavaPairRDD<String, Integer> counts = textFile2
	                .flatMap((String s) -> Arrays.asList(s.toLowerCase().split(",")[0]).iterator())
	                .mapToPair((String s) -> new Tuple2<String, Integer>(s, 1))
	                .reduceByKey((Integer a, Integer b) -> a + b);
	        counts.saveAsTextFile(outputFile);
			break;
		case 2:
			//Section A : The Program by using the low-level API for following request. Retrieve all the user ratings of each movie directed by Martin Brest.
			JavaPairRDD<String,Integer> movie_id = textFile
					.filter((String s) -> s.toLowerCase().split(",")[0].equalsIgnoreCase("Martin Brest"))
					.mapToPair((String s) -> new Tuple2<String, Integer> (s.toLowerCase().split(",")[6],1));

			JavaPairRDD<String, String> ratingsWithId = textFile2
					.mapToPair((String s) -> new Tuple2<String, String>(s.toLowerCase().split(",")[0], s.toLowerCase().split(",")[2]))
					.reduceByKey((String a,String b) -> a +"-"+ b);

			JavaPairRDD<String, Tuple2<Integer, String>> combination = movie_id.join(ratingsWithId);
			combination.saveAsTextFile(outputFile);
			break;
		case 3:
			// Section A : The Program by using the low-level API for following request. Retrieve all the movies produced in 2000 whose average user rating is greater than 2.5.
			String header = textFile2.first(); //extract header

			JavaPairRDD<String,String> movie_id2 = textFile
					.filter((String s) -> s.toLowerCase().split(",")[2].equals("2000"))
					.mapToPair((String s) -> new Tuple2<String, String> (s.toLowerCase().split(",")[6],s.toLowerCase().split(",")[2]));

			JavaPairRDD<String, Integer> countWithId = textFile2
					.filter((String s) -> !s.equalsIgnoreCase(header))
					.mapToPair((String s) -> new Tuple2<String,Integer>(s.toLowerCase().split(",")[0],1))
					.reduceByKey((Integer a, Integer b) -> a + b); 

			JavaPairRDD<String, Double> averageWithId = textFile2
					.filter((String s) -> !s.equalsIgnoreCase(header))
					.mapToPair((String s) -> new Tuple2<String,Double>(s.toLowerCase().split(",")[0], Double.valueOf(s.toLowerCase().split(",")[2])))
					.reduceByKey((Double a, Double b) -> a + b);

			JavaPairRDD<String, Tuple2<Integer, Double>> combination2 = countWithId.join(averageWithId);
			JavaPairRDD<String, Double> averagePair = combination2
					.mapToPair(getAverageByKey)
					.filter((Tuple2<String, Double> s) -> s._2 >= 2.5);

			JavaPairRDD<String, Tuple2<String, Double>> combination3 = movie_id2.join(averagePair);

			combination3.saveAsTextFile(outputFile);
			break;
		case 4:
			// Section B : The Program by using the structured API for following request. Retrieve, for each movie, the number of users that rated it.
			// Create a Spark Session object and set the name of the application
			SparkSession ss= SparkSession.builder().appName("Query4 SparkSQL").getOrCreate();
			// Create a DataFramefrom rating.csv
			DataFrameReader dfr=ss.read().format("csv").option("header", true).option("inferSchema", true);
			Dataset<Row> df= dfr.load(inputFile2);

			Dataset<Row> result = df.groupBy("movieid").count().orderBy("count");

			result.coalesce(1).write().format("com.databricks.spark.csv").save(outputFile);	
			break;
		default:
			System.out.println("You entered unexpected input type. Please try again by entering the numbers that menu has.");
		}
	};
	private static PairFunction<Tuple2<String, Tuple2<Integer, Double>>,String,Double> getAverageByKey = (tuple) -> {
		Tuple2<Integer, Double> val = tuple._2;
		int count = val._1;
		Double total = val._2;
		Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
		return averagePair;
	};
};

