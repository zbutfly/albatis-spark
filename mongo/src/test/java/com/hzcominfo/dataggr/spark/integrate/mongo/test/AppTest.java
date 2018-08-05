package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.spark.io.SparkConnection;
import net.butfly.albatis.spark.io.SparkInput;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");
		try (SparkConnection client = new SparkConnection("mongodb-apptest", uri); SparkInput in = client.input(uri);) {
			in.open();
			in.dataset().foreach(System.out::println);
		}
	}
}
