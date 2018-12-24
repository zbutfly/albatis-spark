package com.hzcominfo.search.collision;

public class CollisionExecutorCache {

	private static int running = 0;

	public synchronized static int getRunning() {
		return running;
	}

	public synchronized static void setRunning(int running) {
		CollisionExecutorCache.running = running;
	}
}
