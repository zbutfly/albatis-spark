package net.butfly.albatis.kafka;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;

@Schema("kafka")
public class SparkKafkaOutput extends SparkOutput {
	private static final long serialVersionUID = 9003837433163351306L;

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
	public void process(Rmap v) {
		// TODO Auto-generated method stub
	}
}
