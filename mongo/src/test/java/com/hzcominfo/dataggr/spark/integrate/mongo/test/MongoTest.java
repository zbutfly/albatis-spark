package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.mongodb.SparkMongoOutput;

public class MongoTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.set("spark.mongodb.input.uri", "mongodb://user:pwd@localhost:80/db.tbl");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("MongoTest").config(conf).getOrCreate();

		Map<String, String> options = Maps.of();
		options.put("partitioner", "MongoSamplePartitioner");
		options.put("uri", "mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb");
		options.put("database", "devdb");
		options.put("collection", "PH_ZHK_CZRK");

		URISpec uriSpec = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb");


		SparkMongoOutput mongoOutput = new SparkMongoOutput(spark,uriSpec);
		mongoOutput.enqueue(Sdream.of());

//		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options);
//
//		// Dataset<R> dataset = MongoSpark.load(jsc, readConfig).toDF();
//		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, readConfig);// ZJHM
//		System.out.println(rdd.first().toJson());
	}
}
