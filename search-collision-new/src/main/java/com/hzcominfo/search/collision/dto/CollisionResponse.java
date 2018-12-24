package com.hzcominfo.search.collision.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CollisionResponse implements Serializable {
	private static final long serialVersionUID = 1525834203508596276L;
	private Integer code;
	private String msg;
	private String percentage;
	private String taskId;
	private Long spends;
	private Integer total;
	private Integer resultCode;
	private List<Map<String, Object>> rows;
	private Set<String> idRows;
	private String fileUrl;
	private Object remark;
	private Integer serialNum;

	public Integer getResultCode() {
		return resultCode;
	}

	public CollisionResponse setResultCode(Integer resultCode) {
		this.resultCode = resultCode;
		return this;
	}

	public Integer getCode() {
		return code;
	}

	public CollisionResponse setCode(Integer code) {
		this.code = code;
		return this;
	}

	public String getMsg() {
		return msg;
	}

	public CollisionResponse setMsg(String msg) {
		this.msg = msg;
		return this;
	}

	public String getPercentage() {
		return percentage;
	}

	public void setPercentage(String percentage) {
		this.percentage = percentage;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public Long getSpends() {
		return spends;
	}

	public void setSpends(Long spends) {
		this.spends = spends;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public List<Map<String, Object>> getRows() {
		return rows;
	}

	public void setRows(List<Map<String, Object>> rows) {
		this.rows = rows;
	}

	public Set<String> getIdRows() {
		return idRows;
	}

	public void setIdRows(Set<String> idRows) {
		this.idRows = idRows;
	}

	public String getFileUrl() {
		return fileUrl;
	}

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

	public Object getRemark() {
		return remark;
	}

	public void setRemark(Object remark) {
		this.remark = remark;
	}
}
