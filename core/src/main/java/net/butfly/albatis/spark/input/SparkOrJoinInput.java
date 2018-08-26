package net.butfly.albatis.spark.input;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol) {
		super(input, col, joined, joinedCol, "full");
	}
}
