package net.butfly.albatis.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public enum SparkJoinType {
	INNER("inner"), LEFT("left"), LEFT_ANTI("left_anti"), FULL("full"), // implemented
	@Deprecated
	LEFT_OUTER("left_outer"), @Deprecated
	LEFT_SEMI("left_semi"), @Deprecated
	RIGHT("right"), @Deprecated
	RIGHT_OUTER("right_outer"), @Deprecated
	OUTER("outer"), @Deprecated
	FULL_OUTER("full_outer");

	public final String type;

	private SparkJoinType(String type) {
		this.type = type;
	}

	public static SparkJoinType of(int value) {
		switch (value) {
		case 1:
			return INNER;
		case 2:
			return FULL;
		case 3:
			return LEFT_ANTI;
		default:
			throw new IllegalArgumentException("JoinType [" + value + "] is not supported.");
		}
	}

	Dataset<Row> join(Dataset<Row> left, String leftCol, Dataset<Row> right, String rightCol) {
		return left.distinct().join(right.distinct(), left.col(leftCol).equalTo(right.col(rightCol)), type);
	}
}
