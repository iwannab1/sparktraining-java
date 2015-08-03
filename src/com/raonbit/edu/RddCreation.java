package com.raonbit.edu;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RddCreation {

	public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
		public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
			return new Tuple2<Text, IntWritable>(new Text(record._1), new IntWritable(record._2));
		}
	}

	public static void main(String[] args){

		SparkConf conf = new SparkConf().setAppName("RddCreation").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// wholetextfile
		JavaPairRDD<String,String> wholefile = sc.wholeTextFiles("d:\\spark");
		
		List<Tuple2<String,String>> filelist = wholefile.collect();
		for(Tuple2<String,String> list : filelist){
			System.out.println("key : " + list._1);
			System.out.println("key : " + list._2);
		}
		
		//SequenceFile
		List<Tuple2<String, Integer>> input = new ArrayList<Tuple2<String, Integer>>();
	    input.add(new Tuple2<String,Integer>("a", 1));
	    input.add(new Tuple2<String,Integer>("a", 2));
	    input.add(new Tuple2<String,Integer>("b", 3));
	    JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
	    
	    JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
	    result.saveAsTextFile("c:\\temp\\sequence.txt");
	    result.saveAsHadoopFile("c:\\temp\\sequenceHadoop.txt", Text.class, IntWritable.class, SequenceFileOutputFormat.class);
	}
}
