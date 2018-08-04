package com.hzcominfo.dataggr.spark.integrate.es.test;
import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_PH_ZHK_CZRK");
		try (SparkConnection client = new SparkConnection("es-apptest", uri); SparkInput in = client.input(uri);) {
			in.open();
//			in.dataset().foreach(System.out::println);
		}
	}
}
