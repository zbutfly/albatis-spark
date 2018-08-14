package net.butfly.albatis.kafka;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSaveOutput;

@Schema("kafka")
public class SparkKafkaOutput extends SparkSaveOutput {
	private static final long serialVersionUID = 9003837433163351306L;

	SparkKafkaOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public java.util.Map<String, String> options() {
		java.util.Map<String, String> options = Maps.of();
		options.put("kafka.bootstrap.servers", targetUri.getHost());
		options.put("subscribe", table());
		options.put("startingOffsets", "earliest");
		return options;
	}

	@Override
	public String format() {
		return "kafka";
	}
}
