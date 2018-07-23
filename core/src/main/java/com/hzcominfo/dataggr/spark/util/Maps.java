package com.hzcominfo.dataggr.spark.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Maps {

	public static <K, V> ConcurrentMap<K, V> of() {
		return new ConcurrentHashMap<>();
	}
}
