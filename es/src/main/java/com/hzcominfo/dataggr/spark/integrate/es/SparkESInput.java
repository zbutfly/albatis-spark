package com.hzcominfo.dataggr.spark.integrate.es;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput.SparkRmapInput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;

public class SparkESInput extends SparkRmapInput {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkESInput() {
		super();
	}

	public SparkESInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Map<String, String> options() {
		Map<String, String> options = Maps.of();
		InetSocketAddress addr = targetUri.getInetAddrs()[0];
		options.put("cluster.name", targetUri.getUsername());
		options.put("es.nodes", addr.getHostName());
		options.put("es.port", targetUri.getParameter(HTTP_PORT));
		options.put("es.resource", table());
		return options;
	}

	@Override
	protected String format() {
		return "es";
	}

	@Override
	protected String schema() {
		return "es,elasticsearch";
	}

	@Override
	protected String table() {
		return targetUri.getPath();
	}
}
