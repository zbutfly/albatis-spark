package com.hzcominfo.dataggr.spark.io;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;
	
	public SparkInnerJoinInput(SparkInput[] inputs) {
		super("inner", inputs);
	}
}
