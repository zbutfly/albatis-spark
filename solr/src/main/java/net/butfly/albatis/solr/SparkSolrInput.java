package net.butfly.albatis.solr;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.input.SparkDataInput;

@Schema("solr")
public class SparkSolrInput extends SparkDataInput {
	private static final long serialVersionUID = -5201381842972371471L;

	public SparkSolrInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = Maps.of();
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
}
