package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Map;

public class ESInputTest {
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]").setAppName("esInput");

		SparkSession session = SparkSession.builder().config(conf).getOrCreate();

		URISpec uri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_CZRK");

		try (SparkESInput esInput = new SparkESInput(session, uri);) {
			for (Map.Entry<String, String> map : esInput.options().entrySet())
				System.out.println(map.getKey() + "\t" + "value=" + map.getValue());
		}
	}
}
