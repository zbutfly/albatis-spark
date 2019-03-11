package com.hzcominfo.dataggr.spark.integrate.test;

import net.butfly.albacore.io.URISpec;

public class URISpecTest {

	public static void main(String[] args) {
		URISpec u = new URISpec("mongodb://user:pwd@localhost:80/db");
		String file = u.getFile();
		String[] path = u.getPaths();
		String db = null;
		String tbl = null;
		String uri = null;
		if (path.length == 0) {
			db = file;
			uri = u.toString();
		} else {
			db = path[0];
			tbl = file;
			uri = u.getSchema() + "://" +u.getAuthority() + "/" +db;
		}
		System.out.println("db: " + db);
		System.out.println("tbl: " + tbl);
		System.out.println("uri: " + uri);
	}
}
