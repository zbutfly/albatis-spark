package net.butfly.albatis.elastic;

import static net.butfly.albatis.spark.impl.Sparks.split;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
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
		super(spark, targetUri, null, TableDesc.dummy(targetUri.getPath()));
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = Maps.of();
		InetSocketAddress addr = targetUri.getInetAddrs()[0];
		options.put("cluster.name", targetUri.getUsername());
		options.put("es.nodes", addr.getHostName());
		// 默认是tcp的port
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
		List<List<Tuple2<String, Dataset<Row>>>> list = Colls.list(schemaAll().values(), item -> {
			Map<String, String> options = options();
			Dataset<Row> rowDataset = JavaEsSparkSQL.esDF(spark, "es://es632@172.30.10.31:29300/ztry_1219/ztry_1219_01", options);
			return Colls.list(split(rowDataset, false), ds -> new Tuple2<>(item.name, ds.persist(StorageLevel.OFF_HEAP())));
		});
		return Colls.flat(list);
	}
}
