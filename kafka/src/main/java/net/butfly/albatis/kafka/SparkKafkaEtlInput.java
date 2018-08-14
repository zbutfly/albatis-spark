package net.butfly.albatis.kafka;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Deprecated
@Schema("kafka:etl")
public class SparkKafkaEtlInput extends SparkKafkaFieldInput {
	private static final long serialVersionUID = -8077483839198954L;
	private final Map<String, String> keys;

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, Map<String, String> topicKeys) {
		super(spark, targetUri, topicKeys.keySet().toArray(new String[0]));
		this.keys = topicKeys;
	}

	@Override
	protected Rmap filter(Rmap etl) {
		return super.filter(etl(etl, keys));
	}

	public static Rmap etl(Rmap etl, Map<String, String> keys) {
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) etl.get("value");
		String key = keys.get(etl.table());
		key = null == key ? null : (String) value.get(key);
		Rmap r = new Rmap(etl.table(), key, value);
		String op = (String) etl.get("oper_type");
		int opv = null == op ? Integer.parseInt(op) : Op.DEFAULT;
		r.op(opv);
		return r;
	}
}
