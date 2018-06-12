package com.hzcominfo.dataggr.spark.plugin;

import java.io.Serializable;
import java.util.List;

public class PluginConfig implements Serializable {
	private static final long serialVersionUID = -4804424398805110003L;
	private List<String> keys;
	private String maxScore;

	public String getMaxScore() {
		return maxScore;
	}

	public void setMaxScore(String maxScore) {
		this.maxScore = maxScore;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
}
