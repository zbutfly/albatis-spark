package com.hzcominfo.dataggr.spark.plugin;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class PluginConfig implements Serializable {
	private static final long serialVersionUID = -4804424398805110003L;
	private final List<String> keys;
	private final String maxScore;
	private Map<SparkInput, String> collisionInputs;

	public PluginConfig(List<String> keys, String maxScore) {
		this.keys = keys;
		this.maxScore = maxScore;
	}
	
	public PluginConfig(List<String> keys, String maxScore, Map<SparkInput, String> collisionInputs) {
		this(keys, maxScore);
		this.collisionInputs = collisionInputs;
	}
	
	
	public String getMaxScore() {
		return maxScore;
	}

	public List<String> getKeys() {
		return keys;
	}

	public Map<SparkInput, String> getCollisionInputs() {
		return collisionInputs;
	}

	public void setCollisionInputs(Map<SparkInput, String> collisionInputs) {
		this.collisionInputs = collisionInputs;
	}
}
