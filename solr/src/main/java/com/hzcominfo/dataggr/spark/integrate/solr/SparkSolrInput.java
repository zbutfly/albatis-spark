package com.hzcominfo.dataggr.spark.integrate.solr;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;

@Schema("solr")
public class SparkSolrInput extends SparkInput {
	private static final long serialVersionUID = -5201381842972371471L;

	public SparkSolrInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Map<String, String> options() {
		Map<String, String> options = Maps.of();
		options.put("collection", table());
		options.put("zkhost", targetUri.getHost());
		options.put("query", "*:*");
		options.put("sort", "id asc");
		options.put("qt", "/export");
		return options;
	}

	@Override
	protected String format() {
		return "solr";
	}
}
