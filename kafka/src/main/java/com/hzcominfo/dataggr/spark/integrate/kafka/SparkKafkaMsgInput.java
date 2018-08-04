package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.R;

@Deprecated
public class SparkKafkaMsgInput extends SparkKafkaEtlInput {
	private static final long serialVersionUID = 3062381213531867738L;

	public SparkKafkaMsgInput() {
		super();
	}

	public SparkKafkaMsgInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected String schema() {
		return "kafka:msg";
	}

	@Override
	protected R etl(Map<String, Object> notetl) {
		return new R(table(), notetl);
	}
}
