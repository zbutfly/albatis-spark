//package com.hzcominfo.search.collision.dto;
//
//import com.hzcominfo.search.collision.mapper.FieldSet;
//import com.hzcominfo.search.collision.util.Config;
//import com.hzcominfo.search.collision.util.MysqlUtils;
//import net.butfly.albacore.utils.logger.Logger;
//
//import java.io.Serializable;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
////Collisionreq表的实体类,用来存储碰撞记录
//public final class CollisionReqDto implements Serializable {
//	private static final long serialVersionUID = -898146346250375571L;
//	private transient static final Logger log = Logger.getLogger(CollisionReqDto.class);
//	private static final String collName = Config.collisionReqColl;
//
//	private String taskId;
//	private String mainFlag;
//	private String tableName;
//	private String tableDisplayName;
//	private String tableConnect;
//	private String queryParam;
//	private String idDefineName;
//	private FieldSet fieldSet;
//	private String selectSize;
//	private String addTime;
//
//
//	public String getMainFlag() {
//		return mainFlag;
//	}
//
//	public void setMainFlag(String mainFlag) {
//		this.mainFlag = mainFlag;
//	}
//
//	public String getTableName() {
//		return tableName;
//	}
//
//	public void setTableName(String tableName) {
//		this.tableName = tableName;
//	}
//
//	public String getTableDisplayName() {
//		return tableDisplayName;
//	}
//
//	public void setTableDisplayName(String tableDisplayName) {
//		this.tableDisplayName = tableDisplayName;
//	}
//
//	public String getTableConnect() {
//		return tableConnect;
//	}
//
//	public void setTableConnect(String tableConnect) {
//		this.tableConnect = tableConnect;
//	}
//
//	public String getSelectSize() {
//		return selectSize;
//	}
//
//	public void setSelectSize(String selectSize) {
//		this.selectSize = selectSize;
//	}
//
//	public String getQueryParam() {
//		return queryParam;
//	}
//
//	public void setQueryParam(String queryParam) {
//		this.queryParam = queryParam;
//	}
//
////	public FieldSet getFieldSet() {
////		return fieldSet;
////	}
////
////	public void setFieldSet(FieldSet fieldSet) {
////		this.fieldSet = fieldSet;
////	}
//
//	public String getAddTime() {
//		return addTime;
//	}
//
//	public void setAddTime(String addTime) {
//		this.addTime = addTime;
//	}
//
//	public String getTaskId() {
//		return taskId;
//	}
//
//	public void setTaskId(String taskId) {
//		this.taskId = taskId;
//	}
//
//	public String getIdDefineName() {
//		return idDefineName;
//	}
//
//	public void setIdDefineName(String idDefineName) {
//		this.idDefineName = idDefineName;
//	}
//
//	public static class CollisionReqMapper {
//		private static String taskId = "TASK_ID";
//		private static String mainFlag = "MAIN_FLAG";
//		private static String tableName = "TABLE_NAME";
//		private static String tableDisplayName = "TABLE_DISPLAY_NAME";
//		private static String tableConnect = "TABLE_CONNECT";
//		private static String selectSize = "SELECT_SIZE";
//		private static String idDefineName = "ID_DEFINE_NAME";
//		private static String queryParam = "QUERY_PARAM";
//		private static String fieldSet = "FIELD_SET";
//		private static String addTime = "ADD_TIME";
//	}
//}
