package com.hzcominfo.dataggr.spark.join;

import java.util.Map;

import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInputBase;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkInnerJoinInput(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}
}
