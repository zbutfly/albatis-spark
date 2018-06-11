package com.hzcominfo.dataggr.spark.io;

public class SparkNonJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;
	
	public SparkNonJoinInput(SparkInput[] inputs) {
		super("leftanti", inputs);
	}
}
