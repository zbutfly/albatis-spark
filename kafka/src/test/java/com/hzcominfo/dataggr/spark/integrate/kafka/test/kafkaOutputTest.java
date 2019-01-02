package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.Map;

import org.apache.spark.SparkConf;
//import net.butfly.albatis.spark.impl.SparkConf;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.kafka.SparkKafkaOutput;

/**
 * Created by 党楚翔 on 2018/11/28.
 */
public class kafkaOutputTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Simulation");
		// Map<String, String> options = Maps.of();
		// options.put("kafka.bootstrap.servers", "data01:9092,data02:9092,data03:9092");
		// options.put("subscribe", "ZHK_QBZX_LGZS_NEW");

		SparkSession session = SparkSession.builder().config(conf).getOrCreate();

		URISpec uriSpec = new URISpec("zk://data01:2181,data02:2181,data03:2181/kafka");

		try (SparkKafkaOutput sparkKafkaOutput = new SparkKafkaOutput(session, uriSpec, TableDesc.dummy("kafkaTest"));) {
			for (Map.Entry<String, String> map : sparkKafkaOutput.options("test1").entrySet())
				System.out.println(map.getKey() + "\t" + "value=" + map.getValue());
		}

	}
}
