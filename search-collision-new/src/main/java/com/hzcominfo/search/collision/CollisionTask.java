package com.hzcominfo.search.collision;

import java.io.Serializable;

import net.butfly.albacore.paral.Task;

public final class CollisionTask implements Runnable, Serializable {
	private static final long serialVersionUID = 4871897268534448345L;
	
	final String id;
	private final Task task;
	
	public CollisionTask(String id, Task task) {
		this.id = id;
		this.task = task;
	}
	
	@Override
	public void run() {
		task.run();
	}
}
