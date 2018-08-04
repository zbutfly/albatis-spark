package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.R.Op;

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
	protected R filter(R etl) {
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) etl.get("value");
		String key = keys.get(etl.table());
		key = null == key ? null : (String) value.get(key);
		R r = new R(etl.table(), key, value);
		String op = (String) etl.get("oper_type");
		int opv = null == op ? Integer.parseInt(op) : Op.DEFAULT;
		r.op(opv);
		return super.filter(r);
	}
}
