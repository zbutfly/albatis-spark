package net.butfly.albatis.kafka;

import static net.butfly.albatis.spark.impl.Sparks.ENC_RMAP;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaZkParser;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.impl.Sparks.SchemaSupport;
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
	protected List<Dataset<Row>> load() {
		Dataset<Rmap> ds = super.load().get(0).map(this::kafka, ENC_RMAP);
		List<Dataset<Row>> r = Colls.list();
		List<TableDesc> keys = Colls.list(tables());
		while (!keys.isEmpty()) {
			TableDesc tt = keys.remove(0);
			Dataset<Row> tds;
			if (keys.isEmpty()) tds = SchemaSupport.rmap2row(tt, ds);
			else {
				tds = SchemaSupport.rmap2row(tt, ds.filter(rr -> tt.name.equals(rr.table())));
				ds = ds.filter(rr -> !tt.name.equals(rr.table())).persist();
			}
			// tds = tds.drop(SchemaSupport.ROW_TABLE_NAME_FIELD, SchemaSupport.ROW_KEY_FIELD_FIELD, SchemaSupport.ROW_KEY_VALUE_FIELD);
			r.add(tds.repartition(col(SchemaSupport.ROW_KEY_VALUE_FIELD)).alias(tt.name));
		}
		return r;
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
