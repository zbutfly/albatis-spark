package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.Maps;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class SparkMongoInput extends SparkInput {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	public SparkMongoInput() {
		super();
	}

	@Override
	protected Dataset<Row> load() {
		return MongoSpark.load(jsc, ReadConfig.create(jsc).withOptions(options())).toDF();
	}

	@Override
	protected java.util.Map<String, String> options() {
		String file = targetUri.getFile();
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String collection = file;
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		java.util.Map<String, String> options = Maps.of();
		options.put("partitioner", "MongoSamplePartitioner");
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		return options;
	}

	@Override
	protected String schema() {
		return "mongodb";
	}

	@Override
	public void dequeue(Consumer<Stream<Row>> using, int batchSize) {
		throw new UnsupportedOperationException();
	}
}
