package com.hzcominfo.search.collision.mapper;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.hzcominfo.search.collision.util.Config;
import com.hzcominfo.search.collision.util.MysqlUtils;

import net.butfly.albacore.utils.logger.Logger;

//Collisionreq表的实体类,用来存储碰撞记录
public final class CollisionReq implements Serializable {
	private static final long serialVersionUID = -898146346250375571L;
	private transient static final Logger log = Logger.getLogger(CollisionReq.class);
	private static final String collName = Config.collisionReqColl;

	private String taskId;
	private String mainFlag;
	private String tableName;
	private String tableDisplayName;
	private String tableConnect;
	private String queryParam;
	private String idDefineName;
	private String fieldSet;
	private String selectSize;
	private String addTime;
	private String hermano;



	public static boolean insert(CollisionReq cRequest) {
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("INSERT INTO ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" (");
		sqlBuffer.append(CollisionReqMapper.taskId);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.mainFlag);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.hermano);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.tableName);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.tableDisplayName);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.tableConnect);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.selectSize);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.idDefineName);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.queryParam);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.fieldSet);
		sqlBuffer.append(",");
		sqlBuffer.append(CollisionReqMapper.addTime);
		sqlBuffer.append(") VALUES('");
		sqlBuffer.append(cRequest.getTaskId());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getMainFlag());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getHermano());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getTableName());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getTableDisplayName());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getTableConnect());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getSelectSize());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getIdDefineName());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getQueryParam());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getFieldSet());
		sqlBuffer.append("','");
		sqlBuffer.append(cRequest.getAddTime());
		sqlBuffer.append("')");

		try (Connection conn = Config.ds.getConnection()) {
			return MysqlUtils.insert(conn, sqlBuffer.toString());
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}

	public static List<CollisionReq> selectCReqByTaskId(String taskId) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("SELECT * FROM ");
		sqlBuffer.append(collName);
		sqlBuffer.append(" WHERE ");
		sqlBuffer.append(CollisionReqMapper.taskId);
		sqlBuffer.append("='");
		sqlBuffer.append(taskId);
		sqlBuffer.append("'");

		try (Connection conn = Config.ds.getConnection()) {
			List<Map<String, Object>> mapList = MysqlUtils.select(conn, sqlBuffer.toString());
			if (mapList == null)
				return null;
			List<CollisionReq> cRequests = new ArrayList<>();
//			进来的数据直接给转成req对象去操作了
			mapList.stream().forEach(m -> cRequests.add(loadFromMap(m)));
			return cRequests;
		} catch (SQLException e) {
			throw new RuntimeException("ds get connection error: ", e);
		}
	}

	private static CollisionReq loadFromMap(Map<String, Object> map) {
		if (map == null) {
			log.info("get an empty CollisionReq");
			return null;
		}
		CollisionReq cRequest = new CollisionReq();
		cRequest.setTaskId((String) map.get(CollisionReqMapper.taskId));
		cRequest.setMainFlag((String) map.get(CollisionReqMapper.mainFlag));
		cRequest.setTableName((String) map.get(CollisionReqMapper.tableName));
		cRequest.setTableDisplayName((String) map.get(CollisionReqMapper.tableDisplayName));
		cRequest.setTableConnect((String) map.get(CollisionReqMapper.tableConnect));
		cRequest.setSelectSize((String) map.get(CollisionReqMapper.selectSize));
		cRequest.setIdDefineName((String) map.get(CollisionReqMapper.idDefineName));
		cRequest.setQueryParam((String) map.get(CollisionReqMapper.queryParam));
		cRequest.setFieldSet((String) map.get(CollisionReqMapper.fieldSet));
		cRequest.setAddTime((String) map.get(CollisionReqMapper.addTime));
		return cRequest;
	}

	public String getHermano() {
		return hermano;
	}

	public void setHermano(String hermano) {
		this.hermano = hermano;
	}

	public String getMainFlag() {
		return mainFlag;
	}

	public void setMainFlag(String mainFlag) {
		this.mainFlag = mainFlag;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableDisplayName() {
		return tableDisplayName;
	}

	public void setTableDisplayName(String tableDisplayName) {
		this.tableDisplayName = tableDisplayName;
	}

	public String getTableConnect() {
		return tableConnect;
	}

	public void setTableConnect(String tableConnect) {
		this.tableConnect = tableConnect;
	}

	public String getSelectSize() {
		return selectSize;
	}

	public void setSelectSize(String selectSize) {
		this.selectSize = selectSize;
	}

	public String getQueryParam() {
		return queryParam;
	}

	public void setQueryParam(String queryParam) {
		this.queryParam = queryParam;
	}

	public String getFieldSet() {
		return fieldSet;
	}

	public void setFieldSet(String fieldSet) {
		this.fieldSet = fieldSet;
	}

	public String getAddTime() {
		return addTime;
	}

	public void setAddTime(String addTime) {
		this.addTime = addTime;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getIdDefineName() {
		return idDefineName;
	}

	public void setIdDefineName(String idDefineName) {
		this.idDefineName = idDefineName;
	}

	public static class CollisionReqMapper {
		private static String taskId = "TASK_ID";
		private static String mainFlag = "MAIN_FLAG";
		private static String tableName = "TABLE_NAME";
		private static String tableDisplayName = "TABLE_DISPLAY_NAME";
		private static String tableConnect = "TABLE_CONNECT";
		private static String selectSize = "SELECT_SIZE";
		private static String idDefineName = "ID_DEFINE_NAME";
		private static String queryParam = "QUERY_PARAM";
		private static String fieldSet = "FIELD_SET";
		private static String addTime = "ADD_TIME";
		private static String hermano = "HERMANO";
	}
}
