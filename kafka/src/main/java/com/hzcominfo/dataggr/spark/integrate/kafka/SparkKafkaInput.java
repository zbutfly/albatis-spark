package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;
import com.hzcominfo.dataggr.spark.io.SparkInput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.R;
import net.butfly.albatis.kafka.config.KafkaZkParser;

/**
 * <pre>
 *|-- key: binary (nullable = true)
 *|-- value: binary (nullable = true)
 *|-- topic: string (nullable = true)
 *|-- partition: integer (nullable = true)
 *|-- offset: long (nullable = true)
 *|-- timestamp: timestamp (nullable = true)
 *|-- timestampType: integer (nullable = true)
 * </pre>
 */
@Schema("kafka")
public class SparkKafkaInput extends SparkInput {
	private static final long serialVersionUID = 9003837433163351306L;
	private final Function<byte[], Map<String, Object>> conv;

	public SparkKafkaInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		conv = Connection.urider(targetUri);
	}

	@Override
	protected String format() {
		return "kafka";
	}

	@Override
	protected java.util.Map<String, String> options() {
		java.util.Map<String, String> options = Maps.of();
		String[] brokers;
		try (KafkaZkParser k = new KafkaZkParser(targetUri.getHost() + targetUri.getPath())) {
			brokers = k.getBrokers();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		options.put("kafka.bootstrap.servers", String.join(",", brokers));// "data01:9092,data02:9092,data03:9092"
		options.put("subscribe", table());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	protected R conv(Row row) {
		Map<String, Object> kafka = super.conv(row);
		byte[] rowkey = (byte[]) kafka.remove("key");
		byte[] bytes = (byte[]) kafka.remove("value");
		String topic = (String) kafka.remove("topic");
		Map<String, Object> values = null == bytes || bytes.length == 0 ? Maps.of() : conv.apply(bytes);
		if (!kafka.isEmpty()) logger().warn("Kafka raw message contains other fields: " + kafka.toString());
		R r = new R(topic, values);
		if (null != rowkey) r = r.key(new String(rowkey, StandardCharsets.UTF_8));
		return r;
	}
}
