package net.butfly.albatis.spark.impl;

import static org.apache.spark.sql.functions.col;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;

public interface Schemas {
	static final Logger logger = Logger.getLogger(Schemas.class);
	static final Encoder<Rmap> ENC_RMAP = Encoders.kryo(Rmap.class);

	static StructField build(FieldDesc f) {
		return new StructField(f.name, Sparks.fieldType(f.type), true, Metadata.empty());
	}

	static StructType build(TableDesc schema, StructField... extras) {
		int l = schema.fields().length;
		StructField[] sfs = new StructField[l + extras.length];
		for (int i = 0; i < l; i++)
			sfs[i] = build(schema.fields()[i]);
		for (int i = 0; i < extras.length; i++)
			sfs[l + i] = extras[i];
		return new StructType(sfs);
	}

	static TableDesc build(StructType schema) {
		TableDesc t = TableDesc.dummy("");
		for (StructField f : schema.fields())
			t.field(new FieldDesc(t, f.name(), Sparks.valType(f.dataType())));
		return t;
	}

	static StructType build(TableDesc table) {
		// StructField[] extras = extra ? EXTRA_FIELDS_SCHEMA : new StructField[0];
		int l = table.fields().length;
		StructField[] sfs = new StructField[l + EXTRA_FIELDS_SCHEMA.length];
		for (int i = 0; i < l; i++)
			sfs[i] = build(table.fields()[i]);
		for (int i = l; i < sfs.length; i++)
			sfs[i] = EXTRA_FIELDS_SCHEMA[i - l];
		return new StructType(sfs);
	}

	final static String ROW_TABLE_NAME_FIELD = "___table";
	final static String ROW_KEY_VALUE_FIELD = "___key_value";
	final static String ROW_KEY_FIELD_FIELD = "___key_field";
	final static String ROW_OP_FIELD = "___op";
	final static StructField[] EXTRA_FIELDS_SCHEMA = new StructField[] { //
			new StructField(ROW_TABLE_NAME_FIELD, DataTypes.StringType, true, Metadata.empty()) //
			, new StructField(ROW_KEY_VALUE_FIELD, DataTypes.StringType, true, Metadata.empty()) //
			, new StructField(ROW_KEY_FIELD_FIELD, DataTypes.StringType, true, Metadata.empty())//
			, new StructField(ROW_OP_FIELD, DataTypes.IntegerType, true, Metadata.empty())//
	};

	static Rmap row2rmap(Row row) {
		Map<String, Object> m = Maps.of();
		for (int i = 0; i < row.schema().fieldNames().length; i++) {
			if (row.isNullAt(i)) continue;
			String f = row.schema().fieldNames()[i];
			Object v = row.get(i);
			m.put(f, v);
		}
		String t = (String) m.remove(ROW_TABLE_NAME_FIELD);
		String k = (String) m.remove(ROW_KEY_VALUE_FIELD);
		String kf = (String) m.remove(ROW_KEY_FIELD_FIELD);
		int op = m.containsKey(ROW_OP_FIELD) ? ((Number) m.remove(ROW_OP_FIELD)).intValue() : Op.DEFAULT;
		return new Rmap(t, k, m).keyField(kf).op(op);
	}

	static Dataset<Rmap> row2rmap(Dataset<Row> ds) {
		logger.warn("Row transform to Rmap, maybe slowly from here: \n\t" + //
				Colls.list(Thread.currentThread().getStackTrace()).get(2).toString());
		return ds.map(Schemas::row2rmap, ENC_RMAP);
	}

	static Dataset<Row> rmap2row(TableDesc table, Dataset<Rmap> ds) {
		StructType s = build(table);
		return ds.map(r -> map2row(r, s, table.rowkey(), r.op()), RowEncoder.apply(s));
	}

	static Row map2row(Rmap r, StructType s, String keyField, int op) {
		Object[] vs = new Object[s.fields().length];
		for (int i = 0; i < vs.length - EXTRA_FIELDS_SCHEMA.length; i++) {
			Object v = r.get(s.fields()[i].name());
			if (null != v) {
				vs[i] = r.get(s.fields()[i].name());
				// logger.error(s.fields()[i].dataType().toString() + ": " + v.toString() + "{" + v.getClass().toString() + "}");
			}
		}
		vs[vs.length - 4] = r.table();
		vs[vs.length - 3] = null == keyField ? null : r.get(keyField);
		vs[vs.length - 2] = keyField;
		vs[vs.length - 1] = op;
		return new GenericRowWithSchema(vs, s);
	}

	@Deprecated
	static Map<String, Dataset<Row>> compute(Dataset<Row> ds) {
		// List<String> keys = ds.groupBy(ROW_TABLE_NAME_FIELD).agg(count(lit(1)).alias("cnt"))//
		// .map(r -> r.getAs(ROW_TABLE_NAME_FIELD), Encoders.STRING()).collectAsList();

		List<String> keys = ds.dropDuplicates(ROW_TABLE_NAME_FIELD)//
				.select(ROW_TABLE_NAME_FIELD)//
				.map(r -> r.getAs(ROW_TABLE_NAME_FIELD), Encoders.STRING())//
				.collectAsList();
		Map<String, Dataset<Row>> r = Maps.of();
		keys = new ArrayList<>(keys);
		while (!keys.isEmpty()) {
			String t = keys.remove(0);
			Dataset<Row> tds;
			if (keys.isEmpty()) tds = ds;
			else {
				tds = ds.filter(col(ROW_TABLE_NAME_FIELD).equalTo(t));
				ds = ds.filter(col(ROW_TABLE_NAME_FIELD).notEqual(t));
			}
			// tds = tds.drop(ROW_TABLE_NAME_FIELD, ROW_KEY_FIELD_FIELD, ROW_KEY_VALUE_FIELD);
			logger.trace(() -> "Table split finished, got [" + t + "].");// and processing with [" + ds.count() + "] records.");
			r.put(t, tds.repartition(col(ROW_KEY_VALUE_FIELD)));
		}
		return r;
	}

	

	@Deprecated
	static Rmap rawToRmap(Row row) {
		byte[] data = row.getAs("value");
		try (ObjectInputStream oss = new ObjectInputStream(new ByteArrayInputStream(data));) {
			return (Rmap) oss.readObject();
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Sinked row data [" + data.length + "] corrupted.", e);
			throw new RuntimeException(e);
		}
	}
}
