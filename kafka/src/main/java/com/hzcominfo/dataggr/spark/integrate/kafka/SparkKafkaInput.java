package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.spark.io.SparkInput.SparkRmapInput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.R;

public class SparkKafkaInput extends SparkRmapInput {
	private static final long serialVersionUID = 9003837433163351306L;
	private final Function<byte[], Map<String, Object>> conv;

	public SparkKafkaInput() {
		super();
		conv = null;
	}

	public SparkKafkaInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		conv = Connection.urider(targetUri);
	}

	@Override
	protected String format() {
		return "kafka";
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
	protected String schema() {
		return "kafka";
	}

	@Override
	protected R conv(Row row) {
		Map<String, Object> kafka = super.conv(row);
		String rowkey = (String) kafka.remove("key");
		byte[] bytes = (byte[]) kafka.remove("value");
		Map<String, Object> values = null == bytes || bytes.length == 0 ? Maps.of() : conv.apply(bytes);
		if (null != rowkey) values.put(".rowkey", rowkey);
		if (!kafka.isEmpty()) logger().warn("Kafka raw message contains other fields: " + kafka.toString());
		return new R(table(), values);
	}
}
