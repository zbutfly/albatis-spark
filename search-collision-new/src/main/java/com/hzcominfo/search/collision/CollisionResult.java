package com.hzcominfo.search.collision;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollisionResult implements Serializable {
	private static final long serialVersionUID = 2663011662571241856L;
	private static Map<String, List<Map<String, Object>>> resultMap = new HashMap<>();

	public synchronized static void cache(String taskId, List<Map<String, Object>> result) {
		resultMap.put(taskId, result);
	}

	public synchronized static void cache(String taskId, Map<String, Object> result) {
		List<Map<String, Object>> list = resultMap.get(taskId);
		if (null == list)
			list = new ArrayList<>();
		list.add(result);
		resultMap.put(taskId, list);
	}

	public synchronized static List<Map<String, Object>> fetch(String taskId) {
		List<Map<String, Object>> data = resultMap.get(taskId);
		if (data == null || data.isEmpty())
			return new ArrayList<>();
		return data;
	}

	public synchronized static List<Map<String, Object>> fetch(String taskId, int currPage, int pageSize) {
		List<Map<String, Object>> data = resultMap.get(taskId);
		if (data == null || data.isEmpty())
			return new ArrayList<>();
		return new ArrayList<>(data.subList((currPage - 1) * pageSize, currPage * pageSize < data.size() ? currPage * pageSize : data.size()));
	}

	public synchronized static int total(String taskId) {
		List<Map<String, Object>> data = resultMap.get(taskId);
		if (data == null || data.isEmpty())
			return 0;
		return data.size();
	}

	public static boolean isExist(String taskId) {
		return resultMap.containsKey(taskId);
	}
}
