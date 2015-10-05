package com.raonbit.edu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

class MakePair implements FlatMapFunction<Iterator<Integer>, Tuple2<Integer,Integer>> {
	@Override
	public Iterable<Tuple2<Integer, Integer>> call(Iterator<Integer> t) throws Exception {
		TaskContext tc = TaskContext.get();
		System.out.println(String.format("Partition ID : %s, Attempt ID : %s", tc.partitionId(), tc.attemptId()));
		List<Tuple2<Integer,Integer>> res = new ArrayList<Tuple2<Integer,Integer>>();
		Integer pre = t.next();
		while(t.hasNext()){
			Integer cur = t.next();
			Tuple2<Integer,Integer> elem = new Tuple2<Integer,Integer>(pre, cur);
			res.add(elem);
		}
		return res;
	}
}

class SeqOp implements Function2<Integer,Integer,Integer> {

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return Math.max(v1, v2);
	}
	
}

class CombOp implements Function2<Integer,Integer,Integer> {

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + v2;
	}
	
}


public class RDDOperation {
	
	public static <T> void print(String title, JavaRDD<T> rdd){
		
		List<T> result = rdd.collect();
		System.out.print(title + " : ");
		for(T t : result){
			System.out.print(t + ",");
		}
		System.out.println();
	}

	public static <K,V> void pairPrint(String title, JavaPairRDD<K,V> rdd){
		
		List<Tuple2<K,V>> result = rdd.collect();
		System.out.print(title + " : ");
		for(Tuple2<K,V> t : result){
			System.out.print(t._1 + ":" + t._2 + ",");
		}
		System.out.println();
	}

	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RddOperation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
		
		// map
		JavaRDD<Integer> DoubleRDD = rdd.map(new Function<Integer, Integer>(){
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * 2;
			}
		}); 
		
		print("DoubleRDD",DoubleRDD);
		
		// filter
		JavaRDD<Integer> filteredRDD = rdd.filter(new Function<Integer, Boolean>(){
			@Override
			public Boolean call(Integer v1) throws Exception {
				return (v1 % 2 ==0) ;
			}
		}); 
		
		print("filteredRDD",filteredRDD);
		
		// flatMap
		
		JavaRDD<Integer> flatMapRDD = rdd.flatMap(new FlatMapFunction<Integer, Integer>(){
			@Override
			public Iterable<Integer> call(Integer t) throws Exception {
				List<Integer> nt = new ArrayList<Integer>();
				nt.add(t);
				return nt;
			}
			
		});
		print("flatMapRDD",flatMapRDD);

		JavaRDD<Iterable<Integer>> listRDD = rdd.map(new Function<Integer, Iterable<Integer>>(){
			@Override
			public Iterable<Integer> call(Integer t) throws Exception {
				List<Integer> nt = new ArrayList<Integer>();
				nt.add(t);
				return nt;
			}
			
		});
		print("listRDD",listRDD);
		
		// mapPartiton
		JavaRDD<Integer> rdd93 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9), 3);
		JavaRDD<Tuple2<Integer, Integer>> mapPartitioinRDD3 = rdd93.mapPartitions(new MakePair());
		print("mapPartitioinRDD3",mapPartitioinRDD3);
		
		JavaRDD<Integer> rdd92 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9), 2);
		JavaRDD<Tuple2<Integer, Integer>> mapPartitioinRDD2 = rdd92.mapPartitions(new MakePair());
		print("mapPartitioinRDD2",mapPartitioinRDD2);
		
		// sample
		JavaRDD<Integer> sampleRDD = rdd.sample(false, 0.5);
		print("sampleRDD", sampleRDD);
		JavaRDD<Integer> sampleRDD2 = rdd.sample(true, 0.5);
		print("sampleRDD2", sampleRDD2);
		
		// groupByKey
		JavaPairRDD<Integer, Integer> pairRDD = rdd.keyBy(new Function<Integer, Integer>(){
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 % 2;
			}
			
		});
		
		pairPrint("pairRDD", pairRDD);
		JavaPairRDD<Integer, Iterable<Integer>> groupByKeyRDD = pairRDD.groupByKey();
		pairPrint("groupByKeyRDD", groupByKeyRDD);
		
		// reduce 
		Integer total = rdd.reduce(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 - v2;
			}
			
		});
		
		System.out.println(String.format("Result : %s", total));
		
		//reduceByKey
		
		JavaPairRDD<Integer, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		pairPrint("reduceByKeyRDD", reduceByKeyRDD);
		
		// aggregate
		Integer aggr = rdd.aggregate(0, new SeqOp(), new CombOp());
		System.out.println("aggregation result : " + aggr);
		
		JavaRDD<Integer> testRdd = rdd.repartition(3);
		System.out.println("aggregation result : " + testRdd.aggregate(0, new SeqOp(), new CombOp()));
		System.out.println("aggregation result : " + testRdd.aggregate(10, new SeqOp(), new CombOp()));
		
		
		// join
		JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("dog","salmon","rat","elephant"),3);
		JavaPairRDD<Integer,String> stringPairRDD = stringRDD.keyBy(new Function<String, Integer>(){
			@Override
			public Integer call(String v1) throws Exception {
				return v1.length();
			}
			
		});
		
		JavaRDD<String> stringRDD2 = sc.parallelize(Arrays.asList("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3);
		JavaPairRDD<Integer,String> stringPairRDD2 = stringRDD2.keyBy(new Function<String, Integer>(){
			@Override
			public Integer call(String v1) throws Exception {
				return v1.length();
			}
			
		});
		
		pairPrint("join pair rdd : " ,stringPairRDD.join(stringPairRDD2));
		pairPrint("left join pair rdd : " ,stringPairRDD.leftOuterJoin(stringPairRDD2));
		pairPrint("right join pair rdd : " ,stringPairRDD.rightOuterJoin(stringPairRDD2));
		pairPrint("full join pair rdd : " ,stringPairRDD.fullOuterJoin(stringPairRDD2));
		
		// cogroup
		JavaRDD<Integer> first = sc.parallelize(Arrays.asList(1,2,1,3),1);
		JavaPairRDD<Integer, String> second = first.mapToPair(new PairFunction<Integer, Integer, String>(){

			@Override
			public Tuple2<Integer, String> call(Integer t) throws Exception {
				return new Tuple2<Integer,String>(t, "second");
			}
		
			
		});
		JavaPairRDD<Integer, String> third = first.mapToPair(new PairFunction<Integer, Integer, String>(){

			@Override
			public Tuple2<Integer, String> call(Integer t) throws Exception {
				return new Tuple2<Integer,String>(t, "third");
			}
		
			
		});
		
		pairPrint("cogroup : ", second.cogroup(third));


	}

}
