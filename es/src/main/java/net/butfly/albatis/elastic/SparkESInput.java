package net.butfly.albatis.elastic;

import static net.butfly.albatis.spark.impl.Sparks.split;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

@Schema("es")
@SparkConf(key = "spark.es.input.uri", value = "es://127.0.0.1/FxxkMongoSpark.FakeCollection")
public class SparkESInput extends SparkRowInput implements SparkESInterface {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkESInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = esOpts(targetUri);
		InetSocketAddress addr = targetUri.getInetAddrs()[0];
		String resourceStr = options.get("database") + "/" + table().name;
		options.put("cluster.name", targetUri.getUsername());
		options.put("es.nodes", addr.getHostName());
		// 默认是tcp的port
		options.put("es.port", targetUri.getParameter(HTTP_PORT, "29200"));
		options.put("es.resource", resourceStr);
		options.put("es.read.field.include", String.join(",", Colls.list(f -> f.name, table().fields())));
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
			Dataset<Row> rowDataset = JavaEsSparkSQL.esDF(spark, options.get("es.resource"), options);
			return Colls.list(split(rowDataset, false), ds -> new Tuple2<>(item.name, ds));
		});
		return Colls.flat(list);
	}
}
