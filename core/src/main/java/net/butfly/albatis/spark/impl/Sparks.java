package net.butfly.albatis.spark.impl;

import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BYTE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.SHORT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;
import static net.butfly.albatis.ddl.vals.ValType.Flags.VOID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Supplier;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

public interface Sparks {
	static final Logger logger = Logger.getLogger(Sparks.class);
	@SuppressWarnings("rawtypes")
	static final Encoder<Map> ENC_MAP = Encoders.javaSerialization(Map.class);
	static final Encoder<Rmap> ENC_RMAP = Encoders.javaSerialization(Rmap.class);

	static String defaultColl(URISpec u) {
		String file = u.getFile();
		String[] path = u.getPaths();
		String tbl = null;
		if (path.length > 0) tbl = file;
		return tbl;
	};

	static Row mapRow(java.util.Map<String, Object> map) {
		List<StructField> fields = Colls.list();
		map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
		return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
	}

	static Map<String, Object> rowMap(Row row) {
		Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq();
		Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
		String t = (String) map.remove("___table");
		if (null != t) return new Rmap(t, map);
		else return map;
	}

	static Rmap rmap(String table, Row row) {
		return new Rmap(table, Sparks.rowMap(row));
	}

	static DataType classType(Object v) {
		return classType(null == v ? Void.class : v.getClass());
	}

	@SuppressWarnings("deprecation")
	static DataType fieldType(ValType t) {
		if (null == t) return DataTypes.StringType;
		switch (t.flag) {
		case VOID:
			return DataTypes.NullType;
		case UNKNOWN:
			return DataTypes.StringType;
		// basic type: primitive
		case BOOL:
			return DataTypes.BooleanType;
		case CHAR:
			return DataTypes.StringType;
		case BYTE:
			return DataTypes.ByteType;
		case SHORT:
			return DataTypes.ShortType;
		case INT:
			return DataTypes.IntegerType;
		case LONG:
			return DataTypes.LongType;
		case FLOAT:
			return DataTypes.FloatType;
		case DOUBLE:
			return DataTypes.DoubleType;
		// basic type: extended
		case STR:
			return DataTypes.StringType;
		case STRL:
			return DataTypes.StringType;
		case BINARY:
			return DataTypes.BinaryType;
		case DATE:
			return DataTypes.TimestampType;
		// assembly type
		case GEO:
			return DataTypes.StringType;
		default:
			Logger.getLogger(SparkIO.class).warn(t.toString() + " not support for spark sql data type");
			return DataTypes.StringType;
		}
	}

	@SuppressWarnings("deprecation")
	static final Map<String, ValType> DATA_VAL_TYPE_MAPPING = Maps.of(//
			DataTypes.NullType.typeName(), ValType.VOID, //
			DataTypes.StringType.typeName(), ValType.STR, //
			// basic type: primitive
			DataTypes.BooleanType.typeName(), ValType.BOOL, //
			DataTypes.ByteType.typeName(), ValType.BYTE, //
			DataTypes.ShortType.typeName(), ValType.SHORT, //
			DataTypes.IntegerType.typeName(), ValType.INT, //
			DataTypes.LongType.typeName(), ValType.LONG, //
			DataTypes.FloatType.typeName(), ValType.FLOAT, //
			DataTypes.DoubleType.typeName(), ValType.DOUBLE, //
			// basic type: extended
			DataTypes.BinaryType.typeName(), ValType.BIN, //
			DataTypes.TimestampType.typeName(), ValType.DATE//
	);

	@SuppressWarnings("deprecation")
	static ValType valType(DataType t) {
		if (null == t) return ValType.UNKNOWN;
		ValType vt = DATA_VAL_TYPE_MAPPING.get(t.typeName());
		if (null != vt) return vt;
		Logger.getLogger(SparkIO.class).warn(t.toString() + " not support for spark sql data type");
		return ValType.STR;
	}

	static DataType classType(Class<?> c) {
		if (CharSequence.class.isAssignableFrom(c)) return DataTypes.StringType;
		if (int.class.isAssignableFrom(c) || Integer.class.isAssignableFrom(c)) return DataTypes.IntegerType;
		if (long.class.isAssignableFrom(c) || Long.class.isAssignableFrom(c)) return DataTypes.LongType;
		if (boolean.class.isAssignableFrom(c) || Boolean.class.isAssignableFrom(c)) return DataTypes.BooleanType;
		if (double.class.isAssignableFrom(c) || Double.class.isAssignableFrom(c)) return DataTypes.DoubleType;
		if (float.class.isAssignableFrom(c) || Float.class.isAssignableFrom(c)) return DataTypes.FloatType;
		if (byte.class.isAssignableFrom(c) || Byte.class.isAssignableFrom(c)) return DataTypes.ByteType;
		if (short.class.isAssignableFrom(c) || Short.class.isAssignableFrom(c)) return DataTypes.ShortType;
		if (byte[].class.isAssignableFrom(c)) return DataTypes.BinaryType;
		if (Date.class.isAssignableFrom(c)) return DataTypes.DateType;
		if (Timestamp.class.isAssignableFrom(c)) return DataTypes.TimestampType;
		if (Void.class.isAssignableFrom(c)) return DataTypes.NullType;
		// if (CharSequence.class.isAssignableFrom(c)) return
		// DataTypes.CalendarIntervalType;
		if (c.isArray()) return DataTypes.createArrayType(classType(c.getComponentType()));
		// if (Iterable.class.isAssignableFrom(c)) return
		// DataTypes.createArrayType(elementType);
		// if (Map.class.isAssignableFrom(c)) return DataTypes.createMapType(keyType,
		// valueType);
		throw new UnsupportedOperationException(c.getName() + " not support for spark sql data type");
	}

	static <T> Seq<T> dataset(Iterable<T> rows) {
		return JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
	}

	static <RR> Function0<RR> func0(Supplier<RR> f) {
		return new AbstractFunction0<RR>() {
			@Override
			public RR apply() {
				return f.get();
			}
		};
	}

	static <T> scala.collection.Map<String, T> mapizeScala(java.util.Map<String, T> javaMap) {
		return scala.collection.JavaConversions.mapAsScalaMap(javaMap);
	}

	static <T> java.util.Map<String, T> mapizeJava(scala.collection.Map<String, T> scalaMap) {
		return scala.collection.JavaConversions.mapAsJavaMap(scalaMap);
	}

	static <T> java.util.List<T> listJava(scala.collection.Seq<T> scalaSeq) {
		return scala.collection.JavaConversions.seqAsJavaList(scalaSeq);
	}

	static <T> Seq<T> listScala(List<T> javaList) {
		return scala.collection.JavaConversions.asScalaBuffer(javaList);
	}

	static class SchemaSupport {
		static <T> long count(Dataset<T> ds) {
			long t = System.currentTimeMillis();
			long l = ds.count();
			logger.error("Count [" + ds.toString() + "]: " + l + ", spent " + (t / 1000.0) + " s.");
			return l;
		}

		static <T> long countByPartitionally(Dataset<T> ds) {
			long t = System.currentTimeMillis();
			AtomicLong c = new AtomicLong(), pc = new AtomicLong();
			ds.foreachPartition(it -> {
				pc.incrementAndGet();
				it.forEachRemaining(v -> c.incrementAndGet());
			});
			logger.error("Count [" + ds.toString() + "]: " + c.get() + " in " + pc.get() + " partitions, spent " + (t / 1000.0) + " ms.");
			return c.get();
		}

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

		public static TableDesc build(StructType schema) {
			TableDesc t = TableDesc.dummy("");
			for (StructField f : schema.fields())
				t.field(new FieldDesc(t, f.name(), Sparks.valType(f.dataType())));
			return t;
		}

		public static StructType build(TableDesc table) {
			// StructField[] extras = extra ? EXTRA_FIELDS_SCHEMA : new StructField[0];
			int l = table.fields().length;
			StructField[] sfs = new StructField[l + EXTRA_FIELDS_SCHEMA.length];
			for (int i = 0; i < l; i++)
				sfs[i] = build(table.fields()[i]);
			for (int i = l; i < EXTRA_FIELDS_SCHEMA.length; i++)
				sfs[i] = EXTRA_FIELDS_SCHEMA[i];
			return new StructType(sfs);
		}

		public final static String ROW_TABLE_NAME_FIELD = "___table";
		public final static String ROW_KEY_VALUE_FIELD = "___key_value";
		public final static String ROW_KEY_FIELD_FIELD = "___key_field";
		public final static String ROW_OP_FIELD = "___op";
		public final static StructField[] EXTRA_FIELDS_SCHEMA = new StructField[] { //
				new StructField(ROW_TABLE_NAME_FIELD, DataTypes.StringType, true, Metadata.empty()) //
				, new StructField(ROW_KEY_VALUE_FIELD, DataTypes.StringType, true, Metadata.empty()) //
				, new StructField(ROW_KEY_FIELD_FIELD, DataTypes.StringType, true, Metadata.empty())//
				, new StructField(ROW_OP_FIELD, DataTypes.IntegerType, true, Metadata.empty())//
		};

		public static final Rmap row2rmap(Row row) {
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

		public static final Dataset<Rmap> row2rmap(Dataset<Row> ds) {
			return ds.map(SchemaSupport::row2rmap, ENC_RMAP);
		}

		public static Dataset<Row> rmap2row(TableDesc table, Dataset<Rmap> ds) {
			StructType s = build(table);
			return ds.map(r -> map2row(table, r, r.op()), RowEncoder.apply(s));
		}

		public static Row map2row(TableDesc table, Map<String, Object> map, int op) {
			return row0(build(table), map, table.name, table.rowkey(), op);
		}

		public static void byTable(Dataset<Row> ds, BiConsumer<String, Dataset<Row>> using) {
			ds.groupByKey(r -> r.getAs(ROW_TABLE_NAME_FIELD), Encoders.STRING()).keys().collectAsList().forEach(t -> {
				Dataset<Row> tds = ds.filter(ds.col(ROW_TABLE_NAME_FIELD).equalTo(t));
				tds = tds.drop(ROW_TABLE_NAME_FIELD, ROW_KEY_FIELD_FIELD, ROW_KEY_VALUE_FIELD);
				using.accept(t, tds);
			});
		}

		@Deprecated
		public static Rmap rawToRmap(Row row) {
			byte[] data = row.getAs("value");
			try (ObjectInputStream oss = new ObjectInputStream(new ByteArrayInputStream(data));) {
				return (Rmap) oss.readObject();
			} catch (ClassNotFoundException | IOException e) {
				logger.error("Sinked row data [" + data.length + "] corrupted.", e);
				throw new RuntimeException(e);
			}
		}

		private static GenericRowWithSchema row0(StructType s, Map<String, Object> map, String table, String keyField, int op) {
			Object[] vs = new Object[s.fields().length];
			for (int i = 0; i < vs.length - EXTRA_FIELDS_SCHEMA.length; i++)
				vs[i] = map.get(s.fields()[i].name());
			vs[vs.length - 4] = table;
			vs[vs.length - 2] = keyField;
			vs[vs.length - 3] = null == keyField ? null : map.get(keyField);
			vs[vs.length - 1] = op;
			return new GenericRowWithSchema(vs, s);
		}
	}
}
