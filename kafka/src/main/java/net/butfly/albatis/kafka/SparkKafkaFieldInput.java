package net.butfly.albatis.kafka;

import java.util.Enumeration;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Schema("kafka:msg")
public class SparkKafkaFieldInput extends SparkKafkaInput {
	private static final long serialVersionUID = 3062381213531867738L;
	private final Map<String, StructField> struct = Maps.of();

	public SparkKafkaFieldInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Dataset<Rmap> load() {
		Dataset<Rmap> ds = super.load();
		return ds.map(this::filter, $utils$.ENC_R);
	}

	@SuppressWarnings("unchecked")
	public <I extends SparkKafkaFieldInput> I struct(StructType schema) {
		for (StructField f : schema.fields())
			struct.put(f.name(), f);
		return (I) this;
	}

	protected Rmap filter(Rmap r) {
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
