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

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Supplier;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.input.SparkDataInput;
import net.butfly.albatis.spark.util.DSdream;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

public abstract class SparkIO implements IO, Serializable {
	private static final long serialVersionUID = 3265459356239387878L;
	private static final Map<Class<? extends IO>, Map<String, Class<? extends SparkIO>>> ADAPTERS //
			= Maps.of(Input.class, Maps.of(), Output.class, Maps.of());
	static {
		scan();
	}

	public final SparkSession spark;
	public final URISpec targetUri;
	protected final String[] tables;

	protected SparkIO(SparkSession spark, URISpec targetUri, String... table) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;
		String[] t;
		if (table.length > 0) t = table;
		else if (null != targetUri && null != targetUri.getFile()) t = new String[] { targetUri.getFile() };
		else t = new String[0];
		tables = t;
	}

	public Map<String, String> options() {
		return Maps.of();
	}

	public String format() {
		return null;
	}

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> cls = (Class<O>) ADAPTERS.get(Output.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends SparkDataInput> I input(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> cls = (Class<I>) ADAPTERS.get(Input.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
					| IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getTargetException());
			}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	private static void scan() {
		for (Class<? extends SparkIO> cls : Reflections.getSubClasses(SparkIO.class)) {
			Schema schema = cls.getAnnotation(Schema.class);
			if (null != schema) {
				if (Input.class.isAssignableFrom(cls)) reg(Input.class, schema, cls);
				else if (Output.class.isAssignableFrom(cls)) reg(Output.class, schema, cls);
			}
		}
		Logger.getLogger(SparkIO.class).debug("Spark adaptors scanned.");
	}

	private static void reg(Class<? extends IO> io, Schema schema, Class<? extends SparkIO> cls) {
		Logger l = Logger.getLogger(cls);
		for (String s : schema.value())
			ADAPTERS.get(io).compute(s, (ss, existed) -> {
				if (null == existed) {
					l.debug("Spark[Output] schema [" + ss + "] register for class:  " + cls.getName());
					return cls;
				} else {
					Schema s0 = existed.getAnnotation(Schema.class);
					if (s0.priority() > schema.priority()) {
						l.warn("Spark[Output] schema [" + ss + "] conflicted and ingored for class:  " + cls.toString() //
								+ "\n\t(existed: " + existed.getName() + ")");
						return existed;
					} else {
						l.warn("Spark[Output] schema [" + ss + "] conflicted and ingored for class:  " + existed.toString() //
								+ "\n\t(priority: " + cls.getName() + ")");
						return cls;
					}
				}
			});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public static @interface Schema {
		String[] value();

		int priority() default 0;
	}

	public String table() {
		// String[] tables = tables.getValue();
		if (null == tables || tables.length == 0) //
			throw new RuntimeException("No table defined for spark i/o.");
		if (tables.length > 1) Logger.getLogger(this.getClass()).warn("Multiple tables defined [" + tables
				+ "] for spark i/o, now only support the first: [" + tables[0] + "].");
		return tables[0];
	}

	@Override
	public int features() {
		return IO.super.features() | IO.Feature.SPARK;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	protected final DSdream<Rmap> dsd(Sdream<?> s) {
		return DSdream.of(spark.sqlContext(), (Sdream<Rmap>) s);
	}

	protected final <T> DSdream<Rmap> dsd(Sdream<T> s, Function<T, Rmap> conv) {
		if (s instanceof DSdream) return (DSdream<Rmap>) s.map(conv);
		List<Rmap> l = s.map(conv).list();
		Dataset<Rmap> ss = spark.sqlContext().createDataset(l, $utils$.ENC_R);
		return DSdream.of(ss);
	}

	// ==========
	private final List<FieldDesc> schema = Colls.list();

	@Override
	public List<FieldDesc> schema() {
		return schema;
	}

	public StructType schemaSpark() {
		if (schema.isEmpty()) return null;
		return new StructType(Colls.list(schema, //
				f -> new StructField(f.name, $utils$.fieldType(f.type), true, Metadata.empty())).toArray(new StructField[0]));
	}

	@Override
	public void schema(FieldDesc... field) {
		schema.clear();
		schema.addAll(Arrays.asList(field));
	}

	@Override
	public void schemaless() {
		schema.clear();
	}

	protected final static String ROW_TABLE_NAME_FIELD = "___table";
	protected final static String ROW_KEY_VALUE_FIELD = "___key_value";
	protected final static String ROW_KEY_FIELD_FIELD = "___key_field";

	protected final Rmap row2rmap(Row row) {
		Map<String, Object> m = Maps.of();
		$utils$.mapizeJava(row.getValuesMap($utils$.listScala(Arrays.asList(row.schema().fieldNames())))).forEach((k, v) -> {
			if (null != v) m.put(k, v);
		});
		String t = (String) m.remove(ROW_TABLE_NAME_FIELD);
		String k = (String) m.remove(ROW_KEY_VALUE_FIELD);
		String kf = (String) m.remove(ROW_KEY_FIELD_FIELD);
		return new Rmap(t, k, m).keyField(kf);
	}

	protected final Row rmap2row0(Rmap r) {
		StructType s = sparkSchema();
		int l = s.fields().length;
		Object[] vs = new Object[l];
		for (int i = 0; i < l - 1; i++)
			vs[i] = r.get(s.fields()[i].name());
		return new GenericRowWithSchema(vs, s);
	}

	protected final Dataset<Row> rmap2rowDs(Dataset<Rmap> ds) {
		if (schema.isEmpty()) throw new UnsupportedOperationException("No schema io could not map back to row.");
		StructType s = sparkSchema(//
				new StructField(ROW_TABLE_NAME_FIELD, DataTypes.StringType, false, Metadata.empty()), //
				new StructField(ROW_KEY_VALUE_FIELD, DataTypes.StringType, true, Metadata.empty()), //
				new StructField(ROW_KEY_FIELD_FIELD, DataTypes.StringType, true, Metadata.empty()));
		int l = s.fields().length;
		return ds.map(r -> {
			Object[] vs = new Object[l];
			for (int i = 0; i < l - 1; i++)
				vs[i] = r.get(s.fields()[i].name());
			vs[l - 3] = r.table();
			vs[l - 2] = r.key();
			vs[l - 1] = r.keyField();
			return new GenericRowWithSchema(vs, s);
		}, RowEncoder.apply(s));
	}

	protected final Dataset<Row> rmap2rowDs0(Dataset<Rmap> ds) {
		if (schema.isEmpty()) throw new UnsupportedOperationException("No schema io could not map back to row.");
		StructType s = sparkSchema();
		int l = s.fields().length;
		return ds.map(r -> {
			Object[] vs = new Object[l];
			for (int i = 0; i < l - 1; i++)
				vs[i] = r.get(s.fields()[i].name());
			return new GenericRowWithSchema(vs, s);
		}, RowEncoder.apply(s));
	}

	protected final StructType sparkSchema(StructField... extras) {
		StructField[] sfs = new StructField[schema.size() + extras.length];
		for (int i = 0; i < schema.size(); i++)
			sfs[i] = new StructField(schema.get(i).name, $utils$.fieldType(schema.get(i).type), true, Metadata.empty());
		for (int i = 0; i < extras.length; i++)
			sfs[schema.size() + i] = extras[i];
		return new StructType(sfs);
	}

	// ====
	public static interface $utils$ extends Serializable {
		@SuppressWarnings("rawtypes")
		static final Encoder<Map> ENC_MAP = Encoders.javaSerialization(Map.class);
		static final Encoder<Rmap> ENC_R = Encoders.javaSerialization(Rmap.class);

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
			Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala()
					.toSeq();
			Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
			String t = (String) map.remove("___table");
			if (null != t) return new Rmap(t, map);
			else return map;
		}

		static Rmap rmap(String table, Row row) {
			return new Rmap(table, $utils$.rowMap(row));
		}

		static DataType classType(Object v) {
			return classType(null == v ? Void.class : v.getClass());
		}

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

		public static <T> Seq<T> dataset(Iterable<T> rows) {
			return JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
		}

		public static <RR> Function0<RR> func0(Supplier<RR> f) {
			return new AbstractFunction0<RR>() {
				@Override
				public RR apply() {
					return f.get();
				}
			};
		}

		public static <T> scala.collection.Map<String, T> mapizeScala(java.util.Map<String, T> javaMap) {
			return scala.collection.JavaConversions.mapAsScalaMap(javaMap);
		}

		public static <T> java.util.Map<String, T> mapizeJava(scala.collection.Map<String, T> scalaMap) {
			return scala.collection.JavaConversions.mapAsJavaMap(scalaMap);
		}

		public static <T> java.util.List<T> listJava(scala.collection.Seq<T> scalaSeq) {
			return scala.collection.JavaConversions.seqAsJavaList(scalaSeq);
		}

		public static <T> Seq<T> listScala(List<T> javaList) {
			return scala.collection.JavaConversions.asScalaBuffer(javaList);
		}

		public static String debug(Row row) {
			String s = row.schema().simpleString() + "==>";
			for (int i = 0; i < row.schema().fields().length; i++)
				s += row.schema().fields()[i].name() + ":" + row.get(i);
			return s;
		}
	}
}
