package net.butfly.albatis.spark.input;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

import java.util.Set;

public class SparkNonJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkNonJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String as,Set<String> leftSet, Set<String> rightSet,String taskId) {
		super(input, col, joined, joinedCol, "leftanti", as, leftSet, rightSet, taskId);
	}
}
