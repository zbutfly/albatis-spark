package com.hzcominfo.dataggr.spark.util;

import java.util.HashMap;
import java.util.Map;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class InputMapTool {

	private final Map<String, SparkInput> map;
	
	public InputMapTool() {
		this.map = new HashMap<>();;
	}
	
	public InputMapTool append(String k, SparkInput v) {
		map.put(k, v);
		return this;
	}
	
	public Map<String, SparkInput> get() {
		return map;
	}
}
