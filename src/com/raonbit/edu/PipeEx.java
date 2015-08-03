package com.raonbit.edu;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PipeEx {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("PipeEx").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> number = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));
		
		number.pipe("C:\\Users\\iwannab1\\workspace\\SparkTraining\\src\\print.bat").collect();
		
		
		sc.close();

	}

}
