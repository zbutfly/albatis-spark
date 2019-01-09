package net.butfly.albatis.solr;

import static net.butfly.albatis.spark.impl.Sparks.split;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

@Schema("solr")
public class SparkSolrInput extends SparkRowInput implements SparkSolr {
	private static final long serialVersionUID = -5201381842972371471L;

	public SparkSolrInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = solrOpts(targetUri);
		options.put("collection", table().name);
		options.put("zkhost", targetUri.getHost());
		options.put("query", "*:*");
		options.put("sort", "id asc");
		options.put("qt", "/export");
		return options;
	}

	@Override
	public String format() {
		return "solr";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		List<List<Tuple2<String, Dataset<Row>>>> list = Colls.list(schemaAll().values(), item -> {
			Map<String, String> options = options();
			options.put("collection", item.name);
			Dataset<Row> solr = spark.read().format("solr").options(options).load();
			return Colls.list(split(solr, false), ds -> new Tuple2<>(item.name, ds.persist(StorageLevel.OFF_HEAP())));
		});
		return Colls.flat(list);
	}
}
