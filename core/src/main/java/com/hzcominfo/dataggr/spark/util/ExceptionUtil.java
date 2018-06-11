package com.hzcominfo.dataggr.spark.util;

public class ExceptionUtil {

	public static void runtime(String msg, Exception e) {
		throw new RuntimeException(msg + ": " + e);
	}
	
	public static void runtime(String msg) {
		throw new RuntimeException(msg);
	}
}
