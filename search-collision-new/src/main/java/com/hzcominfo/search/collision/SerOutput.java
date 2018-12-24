package com.hzcominfo.search.collision;

import java.io.Serializable;
import java.util.Map;

import net.butfly.albatis.io.OddOutput;

public class SerOutput implements OddOutput<Map<String, Object>>, Serializable {
	private static final long serialVersionUID = -697762741785634474L;
	private final String taskId;
	public SerOutput(String taskId) {
		this.taskId = taskId;
	}

	@Override
	public boolean enqueue(Map<String, Object> v) {
//		logger().debug("out: " + v);
		CollisionResult.cache(taskId, v);
		return true;
	}

	@Override
	public void close() {
		OddOutput.super.close();
	}
}
