package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hzcominfo.dataggr.uniquery.Client;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;



@Schema("solr")
public class SparkSolrInput extends SparkRowInput {
	private static final long serialVersionUID = -5201381842972371471L;

	public SparkSolrInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		String solrdbn = targetUri.getPathAt(0);
		String solruri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/";
		if (null != solrdbn) solruri += solrdbn;
		return Maps.of("uri", solruri, "zkhost", targetUri.getHost(), "query", "*:*", "sort", "id asc", "qt", "/export");
	}

	@Override
	public String format() {
		return "solr";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		return Colls.list(schemaAll().values(), t -> {
			Map<String, String> options = options();
			options.put("collection", t.name);

			String conditionExpr = (String) t.attr("TABLE_QUERYPARAM");
			if (!conditionExpr.isEmpty()){
				Client client = null;
				SolrQuery solr = null;
				ObjectMapper objectMapper = new ObjectMapper();
				String queryCondition = null;
				try {
					client = new Client(new SolrConnection(targetUri));
					solr = (SolrQuery)client.getQueryCondition("select * from "+t.dbname+" where " + conditionExpr + " ", "");
					String solrStr = solr.get("json");
					Map<String,String> map = objectMapper.readValue(solrStr, Map.class);
					queryCondition = map.get("query");
				} catch (IOException e) {
					e.printStackTrace();
				}
				options.put("query", queryCondition);
			}


			if (t.fields().length > 0) options.put("fields", String.join(",", Colls.list(f -> f.attr(Desc.PROJECT_FROM, f.name), t.fields())));
			logger().debug("Loading from solr as: " + options);

			Dataset<Row> ds = spark.read().format("solr").options(options).load();

			logger().trace(() -> "Loaded from solr, schema: " + ds.schema().treeString());
			return new Tuple2<>(t.name, ds);
		});
	}
}
