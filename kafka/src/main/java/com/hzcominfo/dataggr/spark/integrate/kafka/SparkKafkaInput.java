package com.hzcominfo.dataggr.spark.integrate.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;

public class SparkKafkaInput extends SparkInput {
	private static final long serialVersionUID = 9003837433163351306L;

	public SparkKafkaInput() {
		super();
	}

	public SparkKafkaInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Dataset<Row> load() {
		return spark.readStream().format(format()).options(options()).load();
		// Encoder<Row> enc = ds.org$apache$spark$sql$Dataset$$encoder;
		// spark.implicits().newMapEncoder(evidence$3)
		// return FuncUtil.mapize(ds);
	}

	@Override
	protected String format() {
		return "kafka";
	}

	@Override
	protected java.util.Map<String, String> options() {
		java.util.Map<String, String> options = Maps.of();
		options.put("kafka.bootstrap.servers", targetUri.getHost());
		options.put("subscribe", targetUri.getFile());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	protected String schema() {
		return "kafka";
	}
}
