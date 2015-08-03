package com.raonbit.edu;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class MapPartitionsWithIndexEx {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitionsWithIndexEx").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> rawInputRdd = sc.textFile("cjem.csv");

		Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
		    @Override
		    public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
		        if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		JavaRDD<String> inputRdd = rawInputRdd.mapPartitionsWithIndex(removeHeader, false);
		
		sc.close();

	}

}
