package com.hzcominfo.search.collision.mapper;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.hzcominfo.search.collision.CollisionExecutor;
import com.hzcominfo.search.collision.CollisionTask;
import com.hzcominfo.search.collision.util.Config;
import com.hzcominfo.search.collision.util.MysqlUtils;

import net.butfly.albacore.utils.logger.Logger;

//Collision表的实体类
public final class Collision implements Serializable {
	private static final long serialVersionUID = -6633822904153199244L;
	private transient static final Logger log = Logger.getLogger(Collision.class);
	private static final String collName = Config.collisionColl;

//
	public static enum CollisionState  implements Serializable {
		WAITING, RUNNING, SUCCESSED, FAILED, CANCEL, BAD
	}
	
	private String taskId;
	private String taskKey;
	private String userName;
	private String pkiName;
	private String type;
	private String state;
	private String remark;
	private String conditions;
	private String updateTime;
	private String addTime;
//	这个设计非常好,相当于把req对象封装到这里
	private List<CollisionReq> cReqs;
	private String nodes;

	public String getTaskKey() {
		return taskKey;
	}

	public void setTaskKey(String taskKey) {
		this.taskKey = taskKey;
	}

	public String getNodes() {
		return nodes;
	}

	public void setNodes(String nodes) {
		this.nodes = nodes;
	}

//	调用fetch()的就是做了join操作
//	todo 这现在的json结构也可以用,可以查询wait的任务; 在这通过mydql加载的表数据去给exec()做join
	public static synchronized CollisionTask fetch(CollisionState state, CollisionExecutor executor) {
//		SELECT * FROM COLLISION WHERE TASK_STATE ='WAITING' ORDER BY ADD_TIME DESC LIMIT 1
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("SELECT * FROM ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" WHERE ");
		sqlBuffer.append(CollisionMapper.state);
		sqlBuffer.append(" ='");
		sqlBuffer.append(state.toString());
		sqlBuffer.append("' ORDER BY ");
		sqlBuffer.append(CollisionMapper.addTime);
		sqlBuffer.append(" DESC LIMIT 1");
		
		try(Connection conn = Config.ds.getConnection()) {
//			一条wait数据
			Map<String, Object> map = MysqlUtils.selectOne(conn, sqlBuffer.toString());
			if (map == null || map.isEmpty()) return null;
//			从结果集中拿到连接信息,构造一个Collision对象,对象里要有平脏数据的uri和join的字段 要传给exec来做join
			Collision collision = loadFromMap(map);
//			从req中根据taskId,拿到创建任务的condition 拿到tableConnect,给线程池
			List<CollisionReq> cReqs = CollisionReq.selectCReqByTaskId(collision.taskId);
//			req校验
//			todo 如果拿到的list是有顺序的,就编写顺序join的逻辑去跑;
			if (cReqs == null || cReqs.isEmpty() || cReqs.size() < 2) {
				state(collision.getTaskId(), CollisionState.BAD, "collision request is unavaliable!");
				return null;
			}
//			是在这设置了req的list
			collision.setcReqs(cReqs);
//			第二个参数是Runnale对象 这调用了CollisionTask的run() 一次传一个Collision对象
			return new CollisionTask(collision.getTaskId(), () -> executor.exec(collision));
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}
	
	public static synchronized void state(String taskId, CollisionState state, Object remark) {
//		拼接update语句,更新task状态
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("UPDATE ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" SET ");
		sqlBuffer.append(CollisionMapper.updateTime);
		sqlBuffer.append("='");
		sqlBuffer.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		sqlBuffer.append("' , ");
		sqlBuffer.append(CollisionMapper.state);
		sqlBuffer.append("='");
		sqlBuffer.append(state.toString());
		sqlBuffer.append("' , ");
		sqlBuffer.append(CollisionMapper.remark);
		sqlBuffer.append("='");
		sqlBuffer.append(remark(remark));
		sqlBuffer.append("' WHERE ");
		sqlBuffer.append(CollisionMapper.taskId);
		sqlBuffer.append("='");
		sqlBuffer.append(taskId);
		sqlBuffer.append("'");
		try(Connection conn = Config.ds.getConnection()) {
			MysqlUtils.update(conn, sqlBuffer.toString());
		} catch (SQLException e) {
			log.error("ds get connection error: ", e);
		}
	}
	
	private static String remark(Object remark) {
		if (null == remark) return null;
		String str = String.valueOf(remark);
		if (str.contains("'")) str = str.replaceAll("'", "\\'");
		return str;
	}
	
	public static synchronized void end(String taskId) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("UPDATE ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" SET ");
		sqlBuffer.append(CollisionMapper.updateTime);
		sqlBuffer.append("=");
		sqlBuffer.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		sqlBuffer.append(" , ");
		sqlBuffer.append(CollisionMapper.state);
		sqlBuffer.append("='");
		sqlBuffer.append(CollisionState.CANCEL.toString());
		sqlBuffer.append("' , ");
		sqlBuffer.append(CollisionMapper.remark);
		sqlBuffer.append("='");
		sqlBuffer.append("碰撞终止");
		sqlBuffer.append("' WHERE ");
		sqlBuffer.append(CollisionMapper.taskId);
		sqlBuffer.append("='");
		sqlBuffer.append(taskId);
		sqlBuffer.append("'");
		
		try(Connection conn = Config.ds.getConnection()) {
			MysqlUtils.update(conn, sqlBuffer.toString());
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}
	
	public static boolean insert(Collision collision) {
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("INSERT INTO ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" (");
		sqlBuffer.append(CollisionMapper.taskId);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.pkiName);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.userName);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.type);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.conditions);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.state);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.updateTime);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.addTime);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionMapper.remark);
		sqlBuffer.append(") VALUES('");
		sqlBuffer.append(collision.getTaskId());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getPkiName());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getUserName());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getType());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getConditions().replace("'", "\\'"));
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getState());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getUpdateTime());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getAddTime());
		sqlBuffer.append("','");
		sqlBuffer.append(collision.getRemark());
		sqlBuffer.append("')");
		
		try(Connection conn = Config.ds.getConnection()) {
			return MysqlUtils.insert(conn, sqlBuffer.toString());
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}
	
	public static Collision selectByTaskId(String taskId) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("SELECT * FROM ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" WHERE ");
		sqlBuffer.append(CollisionMapper.taskId);
		sqlBuffer.append("='");
		sqlBuffer.append(taskId);
		sqlBuffer.append("' LIMIT 1");
		
		try(Connection conn = Config.ds.getConnection()) {
			Map<String, Object> map = MysqlUtils.selectOne(conn, sqlBuffer.toString());
			if (map == null) return null;
			return loadFromMap(map);
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}
	
	public static List<String> fecthOutmodedIds() {
		/*Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		Date date = cal.getTime();
		DBObject query = new BasicDBObject(CollisionMapper.addTime, new BasicDBObject("$lt", date));
		log.info("fecth outmoded ids before " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
		return coll.distinct(CollisionMapper.id, query);*/
		return null;
	}

//	map转成一个Collision对象
	private static Collision loadFromMap(Map<String, Object> map) {
		if (map == null) {
			log.info("get an empty Collision");
			return null;
		}
		Collision collision = new Collision();
		collision.setTaskId((String) map.get(CollisionMapper.taskId));
		collision.setUserName((String) map.get(CollisionMapper.userName));
		collision.setPkiName((String) map.get(CollisionMapper.pkiName));
		collision.setType((String) map.get(CollisionMapper.type));
		collision.setState((String) map.get(CollisionMapper.state));
		collision.setRemark((String) map.get(CollisionMapper.remark));
		collision.setConditions((String) map.get(CollisionMapper.conditions));
		collision.setUpdateTime((String) map.get(CollisionMapper.updateTime));
		collision.setAddTime((String) map.get(CollisionMapper.addTime));
		return collision;
	}

	public String getPkiName() {
		return pkiName;
	}
	public void setPkiName(String pkiName) {
		this.pkiName = pkiName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}
	public String getAddTime() {
		return addTime;
	}
	public void setAddTime(String addTime) {
		this.addTime = addTime;
	}
	public Object getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public List<CollisionReq> getcReqs() {
		return cReqs;
	}
	public void setcReqs(List<CollisionReq> cReqs) {
		this.cReqs = cReqs;
	}
	public String getConditions() {
		return conditions;
	}
	public void setConditions(String conditions) {
		this.conditions = conditions;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public static class CollisionMapper {
		private static String taskId = "TASK_ID";
		private static String userName = "USER_NAME";
		private static String pkiName = "PKI_NAME";
		private static String type = "TASK_TYPE";
		private static String state = "TASK_STATE";
		private static String remark = "REMARK";
		private static String conditions = "CONDITIONS";
		private static String updateTime = "UPDATE_TIME";
		private static String addTime = "ADD_TIME";

	}
}
