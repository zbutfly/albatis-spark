package com.hzcominfo.dataggr.spark.integrate.solr.test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConnection;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("solr://data01:7181,data02:7181,data03:7181/ORACLE_MONGO_CZRK_DUMP");
		try (SparkConnection client = new SparkConnection(uri); SparkInput<Rmap> in = client.input(uri);) {
			in.open();
			// in.dataset().foreach(System.out::println);
		}
	}
}
