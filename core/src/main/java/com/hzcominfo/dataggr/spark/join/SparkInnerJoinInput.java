package com.hzcominfo.dataggr.spark.join;

import java.util.Map;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;
	
	public SparkInnerJoinInput(SparkInput input, String col, Map<String, SparkInput> joinInputs) {
		super(input, col, joinInputs, "inner");
	}
}
