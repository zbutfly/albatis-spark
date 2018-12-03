package net.butfly.albatis.spark.input;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

//	todo 重构代码,把join改写成通用的,解决SparkInput问题

	public SparkInnerJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol) {
		super(input, col, joined, joinedCol, "inner");
    }

	//	this that condition joinType
//	public SparkInnerJoinInputTest(this, that,other , JoinType type) {
//		super(input, col, joined, joinedCol, JoinType.LEFTJOIN);
//	}

}
