package com.hzcominfo.dataggr.spark.integrate.solr;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.Maps;

import net.butfly.albacore.io.URISpec;

public class SparkSolrInput extends SparkInput {
	private static final long serialVersionUID = -5201381842972371471L;
	
	public SparkSolrInput() {
		super();
	}
	
	public SparkSolrInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Dataset<Row> load() {
		return spark.read().format(format()).options(options()).load();
	}

	@Override
	protected Map<String, String> options() {
		Map<String, String> options = Maps.of();
		options.put("collection", targetUri.getFile());
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

	@Override
	protected String schema() {
		return "solr,zk";
	}

	@Override
	public void dequeue(Consumer<Stream<Row>> using, int batchSize) {
		throw new UnsupportedOperationException();
	}
}
