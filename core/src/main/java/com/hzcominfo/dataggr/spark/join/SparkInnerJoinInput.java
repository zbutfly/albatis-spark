package com.hzcominfo.dataggr.spark.join;

import java.util.List;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;
	
	public SparkInnerJoinInput(List<SparkInput> inputs) {
		super("inner", inputs);
	}
}
