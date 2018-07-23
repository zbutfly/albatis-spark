package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.Map;

import org.apache.spark.SparkConf;

import com.hzcominfo.dataggr.spark.util.Maps;

public class SparkStreamTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Simulation");
		// SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		// SparkContext sc = session.sparkContext();

		Map<String, String> options = Maps.of();
		options.put("kafka.bootstrap.servers", "data01:9092,data02:9092,data03:9092");
		options.put("subscribe", "ZHK_QBZX_LGZS_NEW");

		// JavaDStream<String> words;
	}
}
