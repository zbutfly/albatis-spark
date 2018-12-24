package com.hzcominfo.search.collision.dto;

import java.io.Serializable;

//json结构最外层的key,一级key ,json最终要解析成这样; 接口的传入参数
public class Collisionquest implements Serializable {
	private static final long serialVersionUID = -7145848991829393895L;
	private String userName;
	private String pkiName;
	private String type;
	private String conditions;
	private String cornInfo;
	private String info;
	private String taskId;
	private String taskKey;
	private String nodes;


	public String getNodes() {
		return nodes;
	}

	public void setNodes(String nodes) {
		this.nodes = nodes;
	}

	public String getTaskKey() {
		return taskKey;
	}

	public void setTaskKey(String taskKey) {
		this.taskKey = taskKey;
	}

	public String getCornInfo() {
		return cornInfo;
	}

	public void setCornInfo(String cornInfo) {
		this.cornInfo = cornInfo;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
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

	public Object getConditions() {
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
}
