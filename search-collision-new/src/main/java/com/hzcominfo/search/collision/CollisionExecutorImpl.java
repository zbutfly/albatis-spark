package com.hzcominfo.search.collision;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.alibaba.fastjson.JSON;
import com.hzcominfo.dataggr.spark.collision.CollisionConfig;
import com.hzcominfo.dataggr.spark.collision.SparkCollisionInput;
import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.io.SparkPump;
import com.hzcominfo.dataggr.spark.io.SparkSchedule;
import com.hzcominfo.dataggr.spark.join.SparkJoinInput;
import com.hzcominfo.dataggr.spark.util.InputMapTool;
import com.hzcominfo.search.collision.mapper.Collision;
import com.hzcominfo.search.collision.mapper.Collision.CollisionState;
import com.hzcominfo.search.collision.mapper.CollisionReq;
import com.hzcominfo.search.collision.util.Strs;
import net.butfly.albacore.io.URISpec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CollisionExecutorImpl extends CollisionExecutor implements Serializable {
	private static final long serialVersionUID = -8593140717021655658L;

	public CollisionExecutorImpl() {
	}

	public CollisionExecutorImpl(int cap, SparkConnection conn) {
		super(cap, conn);
	}

	@Override
	public void exec(Collision collision) {
		if (collision == null)
			return;
		logger().debug("Running collision tasks: " + running.incrementAndGet());
		long now = System.currentTimeMillis();
//		用来判断task执行状态
		int exc = 0;
//		Collision对象是从mysql中拿的对象,所以这要getid
		String taskId = collision.getTaskId();
//		拿到reqList,
		List<CollisionReq> cReqList = collision.getcReqs();

		CollisionReq mainReq = cReqList.get(0);
		CollisionReq subReq = cReqList.get(1);
		SparkInput newSparkInputMain = getNewSparkInput(mainReq,subReq);

		CollisionReq mainReq2 = cReqList.get(2);
		CollisionReq subReq2 = cReqList.get(3);
		SparkInput newSparkInputSub = getNewSparkInput(mainReq2,subReq2);

		logger().debug("进行lazyjoin的逻辑");
		try {
			SparkInput resultJoin = conn.innerJoin(newSparkInputMain, mainReq.getIdDefineName(), new InputMapTool().append(newSparkInputSub, subReq.getIdDefineName()).get());
			SerOutput on = new SerOutput(taskId);
			SparkPump<Map<String, Object>> pump = resultJoin.pump(on, new HashMap<>());
			//action操作
//	  		显示设置port
			pump.open();
			logger().debug("starting...");
			Dataset<Row> test = resultJoin.dataset();
			test.show(10);
			int jobId = conn.getJobId();
			SparkSchedule.addJobs(taskId, jobId);
			pump.setJobId(jobId);
			pump.close();
		} catch (Exception e) {
			exc++;
			Collision.state(taskId, CollisionState.FAILED, "pump error");
			logger().error("pump error: ", e);
		} finally {
			logger().debug("end pump.");
		}

		if (exc == 0) {
			logger().debug("result count: " + CollisionResult.total(taskId));
			Collision.state(taskId, CollisionState.SUCCESSED, "result count=" + CollisionResult.total(taskId));
		}
	}

	public SparkInput getNewSparkInput(CollisionReq mainReq, CollisionReq subReqs) {
		URISpec uriMain = new URISpec(shapeUri(mainReq.getTableConnect(), mainReq.getTableName()));
//		conn = new SparkConnection("test",uriMain);
		SparkInput input = conn.input(uriMain);
        input.open();
		Dataset<Row> dataset = input.dataset();
		URISpec uriSub = new URISpec(shapeUri(subReqs.getTableConnect(), subReqs.getTableName()));
		SparkInput subInput = conn.input(uriSub);
//		subInput.open();
//		subInput.dataset();
		SparkJoinInput joinInput = conn.innerJoin(input, mainReq.getIdDefineName(), new InputMapTool().append(subInput, subReqs.getIdDefineName()).get());
		return joinInput;
	}

	//	join出一个新的input


	//	todo 叶子节点直接转成SparkInput返回
	public SparkInput leafNodeTOSparkInput(CollisionReq req){
		URISpec iu = new URISpec(shapeUri(req.getTableConnect(), req.getTableName()));
		SparkInput input = conn.input(iu);
		return input;
	}


//	返回表名信息
	private Set<String> cols(CollisionReq req) {
//		解析fieldSet
		Set<String> fset = JSON.parseObject(req.getFieldSet()).keySet();
		Set<String> items = new HashSet<>();
		fset.stream().forEach(s -> items
//				把表名和是否是主表的信息 fieldSet存到了items这个Set集合里
				.add(Strs.concat(s, " as ", Strs.char_concat("__", req.getTableName(), req.getMainFlag(), s))));
		return items;
	}

	private CollisionReq getMainReq(List<CollisionReq> cReqs) {
		for (CollisionReq cReq : cReqs) {
			if ("0".equals(cReq.getMainFlag())) {
				return cReq;
			}
		}
		logger().error("main req is null!!!");
		return null;
	}

	private List<CollisionReq> getSubReqs(List<CollisionReq> cReqs) {
		List<CollisionReq> subReqs = new ArrayList<>();
		for (CollisionReq cReq : cReqs) {
			if (!"0".equals(cReq.getMainFlag())) {
				subReqs.add(cReq);
			}
		}
		return subReqs;
	}

	private String shapeUri(String uri, String table) {
		return uri.contains("?")
				? Strs.concat(Strs.split_part(uri, "\\?", 1), "/", table, "?", Strs.split_part(uri, "\\?", 2))
				: Strs.concat(uri, "/", table);
	}
}