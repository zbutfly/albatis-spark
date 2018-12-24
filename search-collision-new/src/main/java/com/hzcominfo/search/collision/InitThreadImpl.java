package com.hzcominfo.search.collision;

import com.hzcominfo.dataggr.spark.io.SparkConnection;

import net.butfly.albacore.utils.Config;
import net.butfly.albacore.utils.ConfigSet;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

@Config(value = "search-collision-new.properties", prefix = "search.collision")
public class InitThreadImpl extends Thread implements InitThread {
	private transient static final Logger log = Logger.getLogger(InitThreadImpl.class);
	public static ConfigSet conf = Configs.of("search-collision-new.properties", "search.collision");
	public static int cap = Integer.valueOf(conf.get("cap"));
	private static SparkConnection conn = new SparkConnection(conf.get("spark.appname"));
	
	@Override
	public void run() {
		log.info("======================init collision======================");
		// Listener
		conn.addSparkListener();

		// Executor
		new CollisionExecutorImpl(cap, conn);

//		new CollisionExecutorNewImpl(cap,conn);
	}

	public static void endJob(String taskId) {
		conn.endJob(taskId);
	}
}


