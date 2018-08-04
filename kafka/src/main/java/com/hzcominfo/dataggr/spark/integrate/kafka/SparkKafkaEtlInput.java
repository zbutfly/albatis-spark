package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Enumeration;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.R.Op;

@Deprecated
public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	protected static final Logger logger = Logger.getLogger(SparkKafkaEtlInput.class);
	private final Map<String, StructField> struct = Maps.of();

	public SparkKafkaEtlInput() {
		super();
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected String schema() {
		return "kafka:etl";
	}

	public SparkKafkaEtlInput struct(StructType schema) {
		for (StructField f : schema.fields())
			struct.put(f.name(), f);
		return this;
	}

	@Override
	protected R conv(Row row) {
		R etl = etl(super.conv(row));
		return filter(etl);
	}

	protected R etl(Map<String, Object> etl) {
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) etl.get("value");
		R r = new R(table(), value);
		String op = (String) etl.get("oper_type");
		int opv = null == op ? Integer.parseInt(op) : Op.DEFAULT;
		r.op(opv);
		return r;
	}

	private R filter(R r) {
		if (!struct.isEmpty()) for (Enumeration<String> keys = r.keys(); keys.hasMoreElements();) {
			String f = keys.nextElement();
			StructField s = struct.get(f);
			if (null == s) r.remove(f);
			final Object v = r.get(f);
			if (v instanceof java.util.Date) {
				if (s.dataType() instanceof TimestampType && !(v instanceof java.sql.Timestamp)) //
					r.put(f, new java.sql.Timestamp(((java.util.Date) v).getTime()));
				else if (s.dataType() instanceof DateType && !(v instanceof java.sql.Date)) //
					r.put(f, new java.sql.Date(((java.util.Date) v).getTime()));
			}
		}
		return r;
	}
}
