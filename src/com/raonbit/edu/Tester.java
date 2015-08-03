package com.raonbit.edu;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Tester {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Tester").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaRDD<String> lines = sc.textFile("data.txt");
		//JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
		//JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		List<Integer> list = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd = sc.parallelize(list);
		
		List<Integer> ret = rdd.map(new Function<Integer,Integer>() { public Integer call(Integer i) throws Exception { return Integer.valueOf(i*2); }	}).collect();
		
		sc.close();
	}

}
