//package net.butfly.albatis.spark.input;
//
//import net.butfly.albacore.io.URISpec;
//
//public class SparkInnerJoinInputRefactor extends SparkJoinInputRefactor {
//    /**
//     * @param thisUri   表a
//     * @param thatUri   表b
//     * @param conditionThis join条件
//     * @param conditionThat join条件
//     * @param type      join的方式
//     */
//	//	todo 重构代码,把join改写成通用的,解决SparkInput问题
//	public SparkInnerJoinInputRefactor(URISpec thisUri, URISpec thatUri, String conditionThis,String conditionThat, JoinType type) {
//		super(thisUri,thatUri,conditionThis,conditionThat,JoinType.inner);
//	}
//}
