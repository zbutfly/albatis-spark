package net.butfly.albatis.kafka;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkRmapOutput;
import net.butfly.albatis.spark.io.SparkIO.Schema;

@Schema("kafka")
public class SparkKafkaOutput extends SparkRmapOutput {
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
	public boolean enqueue(Rmap r) {
		// TODO Auto-generated method stub
		return true;
	}
}
