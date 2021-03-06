package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConnection;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_PH_ZHK_CZRK");
		try (SparkConnection client = new SparkConnection(uri);
			 SparkInput<Rmap> in = client.input(uri)) {
			in.open();
			// in.dataset().foreach(System.out::println);
		}
	}
}
