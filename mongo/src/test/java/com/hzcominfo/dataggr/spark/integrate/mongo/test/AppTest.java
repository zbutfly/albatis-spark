package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");
		try (SparkConnection client = new SparkConnection("mongodb-apptest", uri); SparkInput in = client.input(uri);) {
			in.open();
			in.dataset().foreach(System.out::println);
		}
	}
}
