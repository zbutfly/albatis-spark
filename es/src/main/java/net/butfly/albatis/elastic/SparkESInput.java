package net.butfly.albatis.elastic;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkInput;

@Schema({ "es", "elasticsearch" })
public class SparkESInput extends SparkInput {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkESInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri, targetUri.getPath());
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
	public String format() {
		return "es";
	}
}
