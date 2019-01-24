package net.butfly.albatis.spark.input;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

import java.util.Set;

public class SparkleftAntiJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkleftAntiJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String as, Set<String> leftSet, Set<String> rightSet, String taskId) {
		super(input, col, joined, joinedCol, JoinType.left_anti.toString(), as, leftSet, rightSet, taskId);
	}
}
