package com.raonbit.edu.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class CsvEx {
	
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[*]");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new SQLContext(ctx);
	    
	    
	    DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load("c:\\temp\\sample.csv");
	    
	    JavaPairRDD<String,String> pair = df.select("key","value").toJavaRDD().mapToPair(new PairFunction<Row,String, String>(){
			@Override
			public Tuple2<String, String> call(Row t) throws Exception {
				return new Tuple2<String,String>(t.getString(0), t.getString(1));
			}
	    	
	    });
	    
	    JavaPairRDD<String, String> agg = pair.reduceByKey(new Function2<String,String,String>(){

			@Override
			public String call(String v1, String v2) throws Exception {
				return v1+","+v2;
			}
	    	
	    });
	    
	    List<Tuple2<String,String>> list = agg.collect();
	    
	    for(Tuple2<String,String> tuple : list){
	    	System.out.println(String.format("%s : %s", tuple._1, tuple._2));
	    }
	    
	    
	}
}
