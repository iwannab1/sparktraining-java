package com.raonbit.edu;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CogroupEx {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("CogroupEx").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
				new Tuple2<String, String>("Apples", "Fruit"), new Tuple2<String, String>("Oranges", "Fruit"),
				new Tuple2<String, String>("Oranges", "Citrus")));
		JavaPairRDD<String, Integer> prices = sc.parallelizePairs(
				Arrays.asList(new Tuple2<String, Integer>("Oranges", 2), new Tuple2<String, Integer>("Apples", 3)));
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogrouped = categories.cogroup(prices);
		
		List<Tuple2<String, Tuple2<Iterable<String>,Iterable<Integer>>>> result = cogrouped.collect();
		
		for(Tuple2<String, Tuple2<Iterable<String>,Iterable<Integer>>> list : result){
			System.out.println("key : " + list._1);
			System.out.println("key of value: " + list._2._1);
			System.out.println("value of value: " + list._2._2);
		}
		
		sc.close();

	}

}
