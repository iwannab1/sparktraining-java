package com.raonbit.edu;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class AccumulatorEx {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("AccumulatorEx").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		AccumulatorParam<Float> floatAccumulatorParam = new AccumulatorParam<Float>() {
			public Float addInPlace(Float r, Float t) {
				return r + t;
			}

			public Float addAccumulator(Float r, Float t) {
				return r + t;
			}

			public Float zero(Float initialValue) {
				return 0.0f;
			}
		};
		
		final Accumulator<Float> floatAccum = sc.accumulator((Float)10.0f, floatAccumulatorParam);
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		JavaRDD<Integer> doublerdd = rdd.map(new Function<Integer, Integer>(){

			@Override
			public Integer call(Integer v1) throws Exception {
				floatAccum.add(v1.floatValue());
				return Integer.valueOf(v1.intValue() * 2);
			}
			
		});
		
		System.out.println("Accumulator value(after transformation) : " + floatAccum.value());
		
		doublerdd.collect();
		
		System.out.println("Accumulator value(after action) : " + floatAccum.value());
		
		
		

	}

}
