package com.raonbit.edu;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class First {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("First").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readme = sc.textFile("D:\\spark\\README.md");
		JavaRDD<String> lineWithSpark = readme.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains("Spark");
			}
		});

		JavaRDD<String> words = lineWithSpark.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});
		
		words.cache();
		
		JavaRDD<String> containa = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains("a");
			}
		}); 
		
		JavaRDD<String> containb = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains("b");
			}
		}); 
		
		
		System.out.println(String.format("Lines with a : %s, Lines with b : %s", containa.count(), containb.count()));
		
			
		
	}

}
