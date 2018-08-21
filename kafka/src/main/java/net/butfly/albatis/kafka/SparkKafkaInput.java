package net.butfly.albatis.kafka;

import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.build;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.map2row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;
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

	public SparkKafkaInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		conv = Connection.urider(targetUri);
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds = super.load();
		TableDesc t = table();
		StructType s = build(t);
		return ds.map(r -> map2row(kafka(r), s, t.rowkey(), Op.DEFAULT), RowEncoder.apply(build(table())));
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
		options.put("subscribe", String.join(",", schemaAll().keySet()));
		options.put("startingOffsets", "latest");// "earliest"
		options.put("maxOffsetsPerTrigger", "10000");
		return options;
	}

	protected Rmap kafka(Row kafka) {
		byte[] rowkey = kafka.getAs("key");
		byte[] bytes = kafka.getAs("value");
		String topic = kafka.getAs("topic");
		// value->..., oper_type->...
		Map<String, Object> map = null == bytes || bytes.length == 0 ? Maps.of() : conv.apply(bytes);
		Rmap r = new Rmap(topic, map);
		if (null != rowkey) r.key(new String(rowkey, StandardCharsets.UTF_8));
		return r;
	}
}
