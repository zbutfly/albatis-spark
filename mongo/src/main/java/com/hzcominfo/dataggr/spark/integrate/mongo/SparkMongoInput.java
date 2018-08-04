package com.hzcominfo.dataggr.spark.integrate.mongo;

import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;

public class SparkMongoInput extends SparkInput {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	public SparkMongoInput() {
		super();
	}

	@Override
	protected java.util.Map<String, String> options() {
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		java.util.Map<String, String> options = Maps.of();
		options.put("partitioner", "MongoSamplePartitioner");
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", table());
		return options;
	}

	@Override
	protected String schema() {
		return "mongodb";
	}
}
