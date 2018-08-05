package net.butfly.albatis.spark.io;

public class SparkUtils {

	public static SparkConnection getConnection(String name) {
		return new SparkConnection(name);
	}
}
