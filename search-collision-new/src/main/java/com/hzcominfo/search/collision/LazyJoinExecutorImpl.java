//package com.hzcominfo.search.collision;
//
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.hzcominfo.dataggr.spark.io.SparkConnection;
//import com.hzcominfo.dataggr.spark.io.SparkInput;
//import com.hzcominfo.dataggr.spark.io.SparkPump;
//import com.hzcominfo.dataggr.spark.join.SparkJoinInput;
//import com.hzcominfo.dataggr.spark.util.InputMapTool;
//import com.hzcominfo.search.collision.mapper.Collision;
//import com.hzcominfo.search.collision.mapper.CollisionReq;
//import com.hzcominfo.search.collision.util.Strs;
//import net.butfly.albacore.io.URISpec;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import scala.Tuple2;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by 党楚翔 on 2018/12/21.
// */
//public class LazyJoinExecutorImpl extends CollisionExecutor {
//
//    private static final long serialVersionUID = -8593140717021655658L;
//
//    public LazyJoinExecutorImpl() {
//    }
//
//    public LazyJoinExecutorImpl(int cap, SparkConnection conn) {
//        super(cap, conn);
//    }
//
//    @Override
//    public void exec(Collision collision) {
//        logger().debug("这里可以进行最后一步action");
//    }
//
//
////    public void exec(Collision collision) {
//////		从collison拿到reqs对象,再拿到主表的信息去和副表做join
////        List<CollisionReq> collisionReqs = collision.getcReqs();
//////		todo 要有个能join一个nodes[{} {}]的方法,返回一个SparkInput对象; 里边的操作要是lazy getNewNodes()
////        SparkInput nowInput = getNewSparkInput(collisionReqs);
//////		todo 根据二叉树定义join顺序
//////		getNewSparkInput()
////        String taskid = collision.getTaskKey();
////        SerOutput output = new SerOutput(taskid);
////        SparkPump<Map<String, Object>> pump = nowInput.pump(output, new HashMap<>());
////        pump.open();
////        Dataset<Row> dataset1 = nowInput.dataset();
////        dataset1.show(10);
////        pump.close();
//////		conn.innerJoin()
//////		Dataset<Row> ds3 = dataset.join(subDataset, col("GMSFHM_s").equalTo(col("GMSFHM_s")), "inner");
////    }
//
//    public SparkInput getNewSparkInput(List<CollisionReq> reqList) {
//        CollisionReq mainReq = getMainReq(reqList);
//        CollisionReq subReqs = getSubReqs(reqList);
//        URISpec uriMain = new URISpec(shapeUri(mainReq.getTableConnect(), mainReq.getTableName()));
//        conn = new SparkConnection("test",uriMain);
//        SparkInput input = conn.input(uriMain);
////        input.open();
////		Dataset<Row> dataset = input.dataset();
//        URISpec uriSub = new URISpec(shapeUri(subReqs.getTableConnect(), subReqs.getTableName()));
//        SparkInput subInput = conn.input(uriSub);
//        SparkJoinInput joinInput = conn.innerJoin(input, mainReq.getIdDefineName(), new InputMapTool().append(subInput, mainReq.getIdDefineName()).get());
//        return joinInput;
//    }
//
//    //	join出一个新的input
//
//
//    //	todo 叶子节点直接转成SparkInput返回
//    public SparkInput leafNodeTOSparkInput(CollisionReq req){
//        URISpec iu = new URISpec(shapeUri(req.getTableConnect(), req.getTableName()));
//        SparkInput input = conn.input(iu);
//        return input;
//    }
//
//    private CollisionReq getMainReq(List<CollisionReq> cReqs) {
//        for (CollisionReq cReq : cReqs) {
//            if ("0".equals(cReq.getMainFlag())) {
//                return cReq;
//            }
//        }
//        logger().error("main req is null!!!");
//        return null;
//    }
//
//
//    //	todo 要有个getsubReq的方法,返回没给
//    private CollisionReq getSubReqs(List<CollisionReq> cReqs) {
//        for (CollisionReq cReq : cReqs) {
//            if (!"0".equals(cReq.getMainFlag())) {
//                return cReq;
//            }
//        }
//        logger().error("subreq is null!!!");
//        return null;
//    }
//
//
//    private String shapeUri(String uri, String table) {
//        return uri.contains("?")
//                ? Strs.concat(Strs.split_part(uri, "\\?", 1), "/", table, "?", Strs.split_part(uri, "\\?", 2))
//                : Strs.concat(uri, "/", table);
//    }
//
//
//}
//
