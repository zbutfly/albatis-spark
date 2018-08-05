package net.butfly.albatis.spark.io;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSchedule {

	private static Map<String, Integer> jobs = new HashMap<>();
	private static Map<String, Object> stages = new HashMap<>();
	private static Map<Object, Integer> taskNums = new HashMap<>();
	private static Map<Object, List<Integer>> tasks = new HashMap<>();
	private static Map<Object, Float> percents = new HashMap<>();

	public static void addJobs(String key, int jobId) {
		jobs.put(key, jobId);
		stages.put(key, jobId);
	}

	public static void addStages(int jobId, List<Object> stageIds) {
		for (String key : stages.keySet()) {
			if (stages.get(key).equals(jobId)) {
				stages.put(key, stageIds);
				break;
			}
		}
	}

	public static void addTaskNums(Object stageId, Integer numTask) {
		taskNums.put(stageId, numTask * 2);
	}

	public static void addTasks(Object stageId, Integer index) {
		List<Integer> taskList = tasks.containsKey(stageId) ? tasks.get(stageId) : new ArrayList<>();
		taskList.add(index);
		tasks.put(stageId, taskList);
		percents.put(stageId, (float) taskList.size() / taskNums.get(stageId) * 100);
	}

	public static void complete(Object stageId) {
		percents.put(stageId, (float) 100);
	}

	@SuppressWarnings("unchecked")
	public static int calcPercent(String sessionId) {
		if (stages.isEmpty())
			return 0;
		if (percents.isEmpty())
			return 5;
		Object obj = stages.get(sessionId);
		if (obj instanceof List) {
			NumberFormat nf = NumberFormat.getInstance();
			nf.setMaximumFractionDigits(0);
			float f = 0;
			List<Object> stageIds = (List<Object>) obj;
			for (Object stageId : stageIds) {
				if (percents.containsKey(stageId))
					f += percents.get(stageId);
			}
			if (f != 0) {
				return Integer.valueOf(nf.format(f / stageIds.size()));
			} else
				return 10;
		}
		return 0;
	}

	public static Integer getJob(String key) {
		return jobs.containsKey(key) ? jobs.get(key) : null;
	}
}
