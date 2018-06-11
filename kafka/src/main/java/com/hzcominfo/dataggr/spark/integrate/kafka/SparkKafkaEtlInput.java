package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import com.hzcominfo.dataggr.spark.util.BytesUtils;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	protected static final Logger logger = Logger.getLogger(SparkKafkaEtlInput.class);
	private StructType etlSchema;

	public SparkKafkaEtlInput() {
		super();
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected String schema() {
		return "kafka:etl";
	}

	public SparkKafkaEtlInput etlSchema(StructType schema) {
		this.etlSchema = schema;
		// this.jsc.broadcast(sch);
		return this;
	}

	@Override
	protected Dataset<Row> load() {
		if (null == etlSchema) throw new IllegalStateException("Etl input need schema of inner map.");
		Dataset<Row> ds = super.load();
		ds = ds.map(this::etl, RowEncoder.apply(etlSchema));
		return ds;
	}

	protected static StructType etlFrameSchema() {
		StructField[] fields = new StructField[4];
		fields[0] = DataTypes.createStructField("key_field", DataTypes.StringType, false);
		fields[1] = DataTypes.createStructField("oper_type", DataTypes.IntegerType, false);
		fields[2] = DataTypes.createStructField("flow_name", DataTypes.StringType, false);
		fields[3] = DataTypes.createStructField("value", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), false);
		StructType schema = new StructType(fields);
		return schema;
	}

	private Row etl(Row r) {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		der.get("oper_type");
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		Map<String, Object> value2 = Maps.of();
		Object v;
		for (StructField f : etlSchema.fields())
			if (null != (v = value.get(f.name()))) {
				if (v instanceof java.util.Date) {
					if (f.dataType() instanceof TimestampType && !(v instanceof java.sql.Timestamp)) //
						v = new java.sql.Timestamp(((java.util.Date) v).getTime());
					else if (f.dataType() instanceof DateType && !(v instanceof java.sql.Date)) //
						v = new java.sql.Date(((java.util.Date) v).getTime());
				}
				value2.put(f.name(), v);
			}
		// if (value.size() != value2.size()) //
		// logger.warn("Fields lost, from: " + value.toString() + " \n\tto: " + value2.toString());
		// logger.trace("etl exacted: " + value.toString());
		return FuncUtil.mapRow(value2);
	}
}
