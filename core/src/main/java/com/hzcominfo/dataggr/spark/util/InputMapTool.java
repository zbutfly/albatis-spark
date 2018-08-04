package com.hzcominfo.dataggr.spark.util;

import java.util.HashMap;
import java.util.Map;

import com.hzcominfo.dataggr.spark.io.SparkInputBase;

@Deprecated
public class InputMapTool {
	private final Map<SparkInputBase, String> map;

	public InputMapTool() {
		this.map = new HashMap<>();
	}

	public InputMapTool append(SparkInputBase k, String v) {
		map.put(k, v);
		return this;
	}

	public Map<SparkInputBase, String> get() {
		return map;
	}
}
