package com.hzcominfo.dataggr.spark.plugin;

import java.util.Map;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class PluginCollision {

	private final Map<String, SparkInput> cInputs;
	
	public PluginCollision(Map<String, SparkInput> cInputs) {
		this.cInputs = cInputs;
	}
	
	public PluginCollision append(String k, SparkInput v) {
		cInputs.put(k, v);
		return this;
	}
	
	public Map<String, SparkInput> get() {
		return cInputs;
	}
}
