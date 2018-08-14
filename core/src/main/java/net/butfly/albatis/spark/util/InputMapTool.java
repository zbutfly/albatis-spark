package net.butfly.albatis.spark.util;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albatis.spark.SparkInput;

@Deprecated
public class InputMapTool {
	private final Map<SparkInput<?>, String> map;

	public InputMapTool() {
		this.map = new HashMap<>();
	}

	public InputMapTool append(SparkInput<?> k, String v) {
		map.put(k, v);
		return this;
	}

	public Map<SparkInput<?>, String> get() {
		return map;
	}
}
