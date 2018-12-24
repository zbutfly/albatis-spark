package com.hzcominfo.search.collision;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.search.collision.mapper.Collision;
import com.hzcominfo.search.collision.mapper.Collision.CollisionState;

import net.butfly.albacore.utils.logger.Loggable;

public abstract class CollisionExecutor implements Loggable {
	protected transient AtomicInteger running;
	private transient boolean closed;
	protected SparkConnection conn;
	protected static String joinCol = "collision_join_key";
	private ExecutorService exec = Executors.newCachedThreadPool();
	protected Future<?> future;

	public CollisionExecutor() {
	}

//	构造器里接收cap和conn参数,创建线程池 线程要有返回值
	public CollisionExecutor(int cap, SparkConnection conn) {
		super();
		this.conn = conn;
		closed = false;
		running = new AtomicInteger(0);
//		给task分配合适的线程数
		future = exec.submit(() -> {
			while (!closed) {
//				正在跑任务的线程数,设置到executor里
				int curr = running.get();
				CollisionExecutorCache.setRunning(curr);
//				如果正在跑的线程数比cap里少,就可以动态增加. fetch
				if (running.get() < cap) {
					CollisionTask task;
//					这很关键,调用了cillision的fetch,拿到了数据
//					CollisionTask fetch = Collision.fetch(CollisionState.WAITING, this);
					if (null != (task = Collision.fetch(CollisionState.WAITING, this))) {
						//如果有等待执行的任务,就执行它 更改taskstatus,
						Collision.state(task.id, CollisionState.RUNNING, null);
						task.run();
					}
				}
			}
		});
	}

	public abstract void exec(Collision collision);
}
