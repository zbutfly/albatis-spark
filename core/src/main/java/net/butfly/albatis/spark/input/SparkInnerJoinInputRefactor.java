package net.butfly.albatis.spark.input;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

public class SparkInnerJoinInputRefactor extends SparkJoinInputRefactor {
	private static final long serialVersionUID = 377289278732441635L;
    /**
     * @param thisUri   表a
     * @param thatUri   表b
     * @param conditionThis join条件
     * @param conditionThat join条件
     * @param type      join的方式
     */
	//	todo 重构代码,把join改写成通用的,解决SparkInput问题
	public SparkInnerJoinInputRefactor(URISpec thisUri, URISpec thatUri, String conditionThis,String conditionThat, JoinType type) {
		super(thisUri,thatUri,conditionThis,conditionThat,JoinType.inner);
	}
}
