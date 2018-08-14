package net.butfly.albatis.spark.input;

import java.util.Map;

import org.apache.spark.sql.Row;

import net.butfly.albatis.spark.SparkInput;

public class SparkNonJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkNonJoinInput(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		super(input, col, joinInputs, "leftanti");
	}
}
