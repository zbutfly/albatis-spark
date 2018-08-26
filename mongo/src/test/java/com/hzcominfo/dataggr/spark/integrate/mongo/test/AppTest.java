package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConnection;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");
		try (SparkConnection client = new SparkConnection(uri); SparkInput<Rmap> in = client.input(uri);) {
			in.open();
			in.vals().forEach(t -> System.out.println(t._2));
		}
	}
}
