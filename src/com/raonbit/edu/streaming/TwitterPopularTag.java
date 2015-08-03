package com.raonbit.edu.streaming;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TwitterPopularTag {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		
	    System.setProperty("twitter4j.oauth.consumerKey", args[0]);
	    System.setProperty("twitter4j.oauth.consumerSecret", args[1]);
	    System.setProperty("twitter4j.oauth.accessToken", args[2]);
	    System.setProperty("twitter4j.oauth.accessTokenSecret", args[3]);

	    SparkConf sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(2000));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);
	    
	    JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status,String>(){
			@Override
			public Iterable<String> call(Status status) throws Exception {
				return Arrays.asList(SPACE.split(status.getText()));
			}
	    	
	    });
	    
	    JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.startsWith("#");
			}
	    });
	    
	    JavaPairDStream<String, Integer> hashTagsOnes = hashTags.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
	    
	    JavaPairDStream<String, Integer> topCount60 = hashTagsOnes.reduceByKeyAndWindow(new Function2<Integer, Integer,Integer>(){
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
	    	
	    }, Durations.seconds(60));
	    
	    JavaPairDStream<String, Integer> topCount60Sorted = topCount60.transformToPair(new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String, Integer>>(){

			@Override
			public JavaPairRDD call(JavaPairRDD<String, Integer> rdd) throws Exception {
				return rdd.sortByKey(false);
			}
	    	
	    });
	    
	    topCount60Sorted.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>(){
			@Override
			public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				List<Tuple2<String, Integer>> topList = rdd.take(10);
				System.out.println(String.format("Popular topics in last 60 seconds (%s total):", rdd.count()));
				for(Tuple2<String, Integer> list : topList){
					System.out.println(String.format("%s (%s tweets)", list._1, list._2));
				}
				return null;
			}
	    	
	    });
	    
	    ssc.start();
	    ssc.awaitTermination();


	}

}
