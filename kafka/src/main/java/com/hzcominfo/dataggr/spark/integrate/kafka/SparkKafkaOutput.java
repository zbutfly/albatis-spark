package com.hzcominfo.dataggr.spark.integrate.kafka;

import net.butfly.albatis.io.R;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;

public class SparkKafkaOutput extends SparkOutput<R> {
	private static final long serialVersionUID = 9003837433163351306L;

	public SparkKafkaOutput() {
		super();
	}

	SparkKafkaOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri);
	}

	@Override
	protected java.util.Map<String, String> options() {
		java.util.Map<String, String> options = Maps.of();
		options.put("kafka.bootstrap.servers", targetUri.getHost());
		options.put("subscribe", table());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	public boolean enqueue(R row) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected String schema() {
		return "kafka";
	}
}
