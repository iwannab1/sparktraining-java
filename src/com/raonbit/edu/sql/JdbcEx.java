package com.raonbit.edu.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JdbcEx {
	
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[*]");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new SQLContext(ctx);
	    
	    
	    String url = "jdbc:mysql://master.raonserver.com/raondb?useUnicode=true&amp;characterEncoding=UTF-8&amp;characterSetResults=UTF-8&amp;autoReconnect=true&amp";
	    String table = "SparkProperty";
	    Properties properties = new Properties();
	    properties.put("driver", "com.mysql.jdbc.Driver");
	    properties.put("user", "raon");
	    properties.put("password", "raonpass");
	    
	    DataFrame df = sqlContext.read().jdbc(url, table, properties);
	    
	    df.show();
	    df.printSchema();
	    
	    JavaRDD<Row> rdd = df.select("name","value").toJavaRDD();
	    
	    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

			@Override
			public void call(Iterator<Row> t) throws Exception {
				String url = "jdbc:mysql://master.raonserver.com/raondb?useUnicode=true&amp;characterEncoding=UTF-8&amp;characterSetResults=UTF-8&amp;autoReconnect=true&amp";
			    Connection con = DriverManager.getConnection(url, "raon", "raonpass");
			    PreparedStatement insert = con.prepareStatement("INSERT INTO SparkTest (name, value) VALUES (?,?)");
				while(t.hasNext()){
					Row r = t.next();
					insert.setString(1,r.getString(0));
					insert.setString(2,r.getString(1));
					insert.executeUpdate();
				}
				con.close();
			}
	    	
	    	
	    });
	    
	    
	    
	    
	    ctx.stop();


	}

}
