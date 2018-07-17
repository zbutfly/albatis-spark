package com.hzcominfo.dataggr.spark.integrate.solr.test;
import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) {
		URISpec uri = new URISpec("solr://data01:7181,data02:7181,data03:7181/ORACLE_MONGO_CZRK_DUMP");
		try (SparkConnection client = new SparkConnection("solr-apptest", uri); SparkInput in = client.input(uri);) {
			in.open();
//			in.dataset().foreach(System.out::println);
		}
	}
}
