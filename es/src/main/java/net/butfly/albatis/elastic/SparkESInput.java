package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.hzcominfo.dataggr.uniquery.Client;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL$;
import scala.Tuple2;

@Schema("es")
public class SparkESInput extends SparkRowInput {
	private static final long serialVersionUID = 5472880102313131224L;

	public SparkESInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = Maps.of();
		options.put("cluster.name", targetUri.getUsername());
//		options.put("es.nodes", targetUri.getHostWithSecondaryPort(1));//TODO direct give it http port; last : to last /
		options.put("es.nodes", targetUri.getHost());
		return options;
	}

	@Override
	public String format() {
		return "es";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		System.setProperty("es.set.netty.runtime.available.processors", "false");
		List<List<Tuple2<String, Dataset<Row>>>> list = Colls.list(schemaAll().values(), t -> {
			Map<String, String> options = options();
			String conditionExpr = (String) t.attr("TABLE_QUERYPARAM");
			if (!Strings.isNullOrEmpty(conditionExpr)){
				Client client = null;
				Object queryCondition = null;
				try {
					client = new Client(new ElasticRestHighLevelConnection(targetUri));
					String indexType = targetUri.getFile() +"."+ t.dbname;
					queryCondition = client.getQueryCondition("select * from "+indexType+" where " + conditionExpr + " ", null);
				} catch (IOException e) {
					e.printStackTrace();
				}
				options.put("es.query", String.valueOf(queryCondition));
			}
			options.put("es.resource", indexAndType(t.name));
			if (t.fields().length > 0) options.put("es.read.field.include", //
					String.join(",", Colls.list(f -> f.attr(Desc.PROJECT_FROM, f.name), t.fields())));
			logger().debug("Loading from elasticsearch as: " + options);
			long start = System.currentTimeMillis();
			Dataset<Row> resultDS = JavaEsSparkSQL.esDF(spark, options.get("es.resource"), options).persist(StorageLevel.MEMORY_AND_DISK());//direct cache it
			logger().trace(() -> "Loaded from elasticsearch, schema: " + resultDS.schema().treeString());
			logger().info("esDS cache use:"+ (System.currentTimeMillis()-start)/1000.0 + "s");
			long count = resultDS.count(); //coalesce(300)  explicit call cache
			logger().info("esDS count use:\t"+(System.currentTimeMillis()-start)/1000.0 + "s"+"\n\tcount:"+count);
            return Colls.list(new Tuple2<>(t.name, resultDS));
		});
		return Colls.flat(list);
	}

	private String indexAndType(String tableName) {
		String indexAndType;
		indexAndType = targetUri.getPathAt(0);
		if (tableName.split("/").length > 1 || null == indexAndType) indexAndType = tableName;
		else indexAndType += ("/" + tableName);
		return indexAndType;
	}
}
