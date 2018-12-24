package com.hzcominfo.search.collision.util;

import java.io.IOException;
import java.io.Serializable;

import com.hzcominfo.search.collision.InitThreadImpl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.utils.ConfigSet;

public class Config implements Serializable {
	private static final long serialVersionUID = 3719554376552784378L;
	public static String collisionColl;
	public static String collisionReqColl;
	public static HikariDataSource ds;
	public static ConfigSet conf = InitThreadImpl.conf;

	static {
		try {
			initParams();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void initParams() throws IOException {
		collisionColl = conf.get("coll.collision");
		collisionReqColl = conf.get("coll.collision_request");
		ds = getDs();
	}

	private static HikariDataSource getDs() {
		HikariConfig config = new HikariConfig();
		String url = conf.get("uri");
		config.setJdbcUrl(url);
		config.setDriverClassName("com.mysql.cj.jdbc.Driver");
		return new HikariDataSource(config);
	}
}
