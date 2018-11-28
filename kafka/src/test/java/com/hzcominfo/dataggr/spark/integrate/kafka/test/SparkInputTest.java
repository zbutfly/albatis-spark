package com.hzcominfo.dataggr.spark.integrate.kafka.test;

import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.kafka.SparkKafkaInput;
import org.apache.spark.SparkConf;


import org.apache.spark.sql.*;

public class SparkInputTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Simulation");
//		Map<String, String> options = Maps.of();
//		options.put("kafka.bootstrap.servers", "data01:9092,data02:9092,data03:9092");
//		options.put("subscribe", "ZHK_QBZX_LGZS_NEW");


        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        URISpec uriSpec = new URISpec("zk://data01:2181,data02:2181,data03:2181/kafka");


        SparkKafkaInput kafkaInput = new SparkKafkaInput(session,uriSpec,TableDesc.dummy("test666"));

        System.out.println(kafkaInput.format());

        Map<String, String> kafkaMap = kafkaInput.options();

        for (Map.Entry<String,String> map : kafkaMap.entrySet()){
            System.out.println("key="+map.getKey()+"\t"+"value="+map.getKey());
        }

	}
}
