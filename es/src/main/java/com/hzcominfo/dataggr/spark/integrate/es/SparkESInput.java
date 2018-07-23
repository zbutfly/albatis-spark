package com.hzcominfo.dataggr.spark.integrate.es;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.Maps;

import net.butfly.albacore.io.URISpec;

public class SparkESInput extends SparkInput {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkESInput() {
		super();
	}
	
	public SparkESInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Dataset<Row> load() {
		return spark.read().format(format()).options(options()).load();
	}

	@Override
	protected Map<String, String> options() {
		Map<String, String> options = Maps.of();
		InetSocketAddress addr = targetUri.getInetAddrs()[0];
		options.put("cluster.name", targetUri.getUsername());
		options.put("es.nodes", addr.getHostName());
		options.put("es.port", targetUri.getParameter(HTTP_PORT));
		options.put("es.resource", targetUri.getPath());
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
	public void dequeue(Consumer<Stream<Row>> using, int batchSize) {
		throw new UnsupportedOperationException();
	}
}
