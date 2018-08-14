package net.butfly.albatis.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaZkParser;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.input.SparkDataInput;

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
public class SparkKafkaInput extends SparkDataInput {
	private static final long serialVersionUID = 9003837433163351306L;
	private final Function<byte[], Map<String, Object>> conv;

	public SparkKafkaInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		conv = Connection.urider(targetUri);
	}

	@Override
	protected Dataset<Rmap> load() {
		Dataset<Rmap> ds = super.load();
		return ds.map(this::kafka, $utils$.ENC_R);
	}

	@Override
	public String format() {
		return "kafka";
	}

	@Override
	public java.util.Map<String, String> options() {
		java.util.Map<String, String> options = Maps.of();
		String[] brokers;
		try (KafkaZkParser k = new KafkaZkParser(targetUri.getHost() + targetUri.getPath())) {
			brokers = k.getBrokers();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		options.put("kafka.bootstrap.servers", String.join(",", brokers));// "data01:9092,data02:9092,data03:9092"
		options.put("subscribe", String.join(",", tables));
		options.put("startingOffsets", "latest");// "earliest"
		options.put("maxOffsetsPerTrigger", "10000");
		return options;
	}

	private Rmap kafka(Rmap kafka) {
		byte[] rowkey = (byte[]) kafka.remove("key");
		byte[] bytes = (byte[]) kafka.remove("value");
		String topic = (String) kafka.remove("topic");
		Map<String, Object> values = null == bytes || bytes.length == 0 ? Maps.of() : conv.apply(bytes);
		// if (!kafka.isEmpty()) logger().trace("Kafka raw message contains other fields: " + kafka.toString());
		Rmap r = new Rmap(topic, values);
		if (null != rowkey) r = r.key(new String(rowkey, StandardCharsets.UTF_8));
		return r;
	}

	private static final StructType schema = new StructType();
	static {
		schema.add("table", DataTypes.StringType);
		schema.add("key", DataTypes.StringType);
		schema.add("data", DataTypes.BinaryType);
	}
}
