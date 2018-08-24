package net.butfly.albatis.elastic;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Schema({ "es", "elasticsearch" })
public class SparkESInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkESInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri, TableDesc.dummy(targetUri.getPath()));
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = Maps.of();
		InetSocketAddress addr = targetUri.getInetAddrs()[0];
		options.put("cluster.name", targetUri.getUsername());
		options.put("es.nodes", addr.getHostName());
		options.put("es.port", targetUri.getParameter(HTTP_PORT));
		options.put("es.resource", table().name);
		return options;
	}

	@Override
	public String format() {
		return "es";
	}

	@Override
	protected Dataset<Row> load() {
		return null;
	}
}
