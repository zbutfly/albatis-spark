package com.hzcominfo.dataggr.spark.io;

public class SparkUtils {

	public static SparkConnection getConnection(String name) {
		return new SparkConnection(name);
	}
}
