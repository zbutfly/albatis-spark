package net.butfly.albatis.spark.impl;

import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELDS;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELDS_LIST;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_KEY_FIELD;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_KEY_VALUE;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_OP;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_TABLE_EXPR;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_TABLE_NAME;
import static net.butfly.albatis.spark.impl.SchemaExtraField.get;
import static net.butfly.albatis.spark.impl.SchemaExtraField.struct;
import static net.butfly.albatis.spark.impl.Sparks.valType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;
import scala.Tuple2;

public interface Schemas {
	static final Logger logger = Logger.getLogger(Schemas.class);
	static final Encoder<Rmap> ENC_RMAP = Encoders.kryo(Rmap.class);

	static StructType build(TableDesc schema, StructField... extras) {
		int l = schema.fields().length;
		StructField[] sfs = new StructField[l + extras.length];
		for (int i = 0; i < l; i++)
			sfs[i] = struct(schema.fields()[i]);
		for (int i = 0; i < extras.length; i++)
			sfs[l + i] = extras[i];
		return new StructType(sfs);
	}

	static TableDesc build(StructType schema) {
		TableDesc t = TableDesc.dummy("");
		for (StructField f : schema.fields())
			t.field(new FieldDesc(t, f.name(), valType(f.dataType())));
		return t;
	}

	static StructType build(TableDesc table) {
		List<StructField> sfs = Colls.list(SchemaExtraField::struct, table.fields());
		sfs.addAll(Colls.list(FIELDS_LIST, f -> FIELDS.get(f).struct));
		return createStructType(sfs);
	}

//
	static Rmap row2rmap(Row row) {
		Map<String, Object> map = Maps.of();
		for (int i = 0; i < row.schema().fieldNames().length; i++) {
			if (row.isNullAt(i)) continue;
			String f = row.schema().fieldNames()[i];
			Object v = row.get(i);
			map.put(f, v);
		}
		String t = (String) map.remove(FIELD_TABLE_NAME);
		String te = (String) map.remove(FIELD_TABLE_EXPR);
		String k = (String) map.remove(FIELD_KEY_VALUE);
		String kf = (String) map.remove(FIELD_KEY_FIELD);
		int op = map.containsKey(FIELD_OP) ? ((Number) map.remove(FIELD_OP)).intValue() : Op.DEFAULT;
		return new Rmap(k, map).table(t, te).keyField(kf).op(op);
	}

	static Dataset<Rmap> row2rmap(Dataset<Row> ds) {
		logger.warn("Row transform to Rmap, maybe slowly from here: \n\t" + //
				Colls.list(Thread.currentThread().getStackTrace()).get(2).toString());
		return ds.map((MapFunction<Row, Rmap>) Schemas::row2rmap, ENC_RMAP);
	}

	static Dataset<Row> rmap2row(TableDesc table, Dataset<Rmap> ds) {
		StructType s = build(table);
		return ds.map((MapFunction<Rmap, Row>) r -> map2row(r, s, table.rowkey(), r.op()), RowEncoder.apply(s));
	}

	static Row map2row(Rmap r, StructType s, String keyField, int op) {
		Object[] vs = new Object[s.fields().length];
		Object v;
		SchemaExtraField ex;
		String n;
		for (int i = 0; i < vs.length - FIELDS.size(); i++)
			if (null != (v = null != (ex = get(n = s.fields()[i].name())) ? ex.getter.apply(r) : r.get(n))) vs[i] = v;
		return new GenericRowWithSchema(vs, s);
	}

	@Deprecated
	static List<Tuple2<String, Dataset<Row>>> compute(Dataset<Row> ds) {
		List<String> keys = ds.dropDuplicates(FIELD_TABLE_NAME).select(FIELD_TABLE_NAME)//
				.map((MapFunction<Row, String>)  r -> r.getAs(FIELD_TABLE_NAME), Encoders.STRING())//
				.collectAsList();
		List<Tuple2<String, Dataset<Row>>> r = Colls.list();
		keys = new ArrayList<>(keys);
		while (!keys.isEmpty()) {
			String t = keys.remove(0);
			Dataset<Row> tds;
			if (keys.isEmpty()) tds = ds;
			else {
				tds = ds.filter(col(FIELD_TABLE_NAME).equalTo(t));
				ds = ds.filter(col(FIELD_TABLE_NAME).notEqual(t));
			}
			logger.trace(() -> "Table split finished, got [" + t + "].");// and processing with [" + ds.count() + "] records.");
			r.add(new Tuple2<>(t, tds.repartition(col(FIELD_KEY_VALUE))));
		}
		return r;
	}

	@Deprecated
	static Rmap rawToRmap(Row row) {
		byte[] data = row.getAs("value");
		try (ObjectInputStream oss = new ObjectInputStream(new ByteArrayInputStream(data))) {
			return (Rmap) oss.readObject();
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Sinked row data [" + data.length + "] corrupted.", e);
			throw new RuntimeException(e);
		}
	}
}
