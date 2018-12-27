package net.butfly.albatis.elastic;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

@Schema({ "es", "elasticsearch" })
public class SparkESInput extends SparkRowInput {
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
//		默认是tcp的port
		options.put("es.port", targetUri.getParameter(HTTP_PORT, "29930"));
		options.put("es.resource", table().name);
		return options;
	}

	@Override
	public String format() {
		return "es";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		return null;
	}
}
