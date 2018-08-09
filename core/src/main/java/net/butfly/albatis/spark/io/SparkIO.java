package net.butfly.albatis.spark.io;

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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.google.common.base.Supplier;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.impl.OutputSink;
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
		else if (null != targetUri.getFile()) t = new String[] { targetUri.getFile() };
		else t = new String[0];
		tables = t;
	}

	protected Map<String, String> options() {
		return null;
	}

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTERS.get(Output.class).get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends SparkInput> I input(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTERS.get(Input.class).get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
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
	protected final DSdream<Rmap> dsd(Sdream<?> s) {
		return DSdream.of(spark.sqlContext(), (Sdream<Rmap>) s);
	}

	protected final <T> DSdream<Rmap> dsd(Sdream<T> s, Function<T, Rmap> conv) {
		if (s instanceof DSdream) return (DSdream<Rmap>) s.map(conv);
		List<Rmap> l = s.map(conv).list();
		Dataset<Rmap> ss = spark.sqlContext().createDataset(l, $utils$.ENC_R);
		return DSdream.of(ss);
	}

	// ====

	public static interface $utils$ extends Serializable {
		@SuppressWarnings("rawtypes")
		public static final Encoder<Map> ENC_MAP = Encoders.javaSerialization(Map.class);
		public static final Encoder<Rmap> ENC_R = Encoders.javaSerialization(Rmap.class);

		public static String defaultColl(URISpec u) {
			String file = u.getFile();
			String[] path = u.getPaths();
			String tbl = null;
			if (path.length > 0) tbl = file;
			return tbl;
		};

		public static Row mapRow(java.util.Map<String, Object> map) {
			List<StructField> fields = Colls.list();
			map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
			return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
		}

		public static Row mapRow(Rmap map) {
			List<StructField> fields = Colls.list();
			map.put("___table", map.table());
			map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
			return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
		}

		public static Map<String, Object> rowMap(Row row) {
			Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala()
					.toSeq();
			Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
			String t = (String) map.remove("___table");
			if (null != t) return new Rmap(t, map);
			else return map;
		}

		public static Rmap rmap(String table, Row row) {
			return new Rmap(table, $utils$.rowMap(row));
		}

		public static DataType classType(Object v) {
			return classType(null == v ? Void.class : v.getClass());
		}

		public static DataType classType(Class<?> c) {
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

		public static <T> scala.collection.Map<String, T> mapizeScala(java.util.Map<String, T> value) {
			return scala.collection.JavaConversions.mapAsScalaMap(value);
		}

		public static <T> java.util.Map<String, T> mapizeJava(scala.collection.Map<String, T> vs) {
			return scala.collection.JavaConversions.mapAsJavaMap(vs);
		}

		/**
		 * Foreach writing (streaming by sink or stocking)
		 */
		public static <T> void each(Dataset<T> ds, Output<T> writer) {
			Logger.getLogger(DSdream.class).info("Dataset writing: " + ds.toString());
			if (ds.isStreaming()) sink(ds, writer.target());
			else if (writer instanceof OddOutput) ds.foreach(((OddOutput<T>) writer)::enqueue);
			else ds.foreach(r -> writer.enqueue(Sdream.of1(r)));
		}

		/**
		 * Sink writing by <code>OutputSink</code>, <code>uri</code> in <code>options()</code> is required.
		 */
		public static <V> void sink(Dataset<V> ds, URISpec uri) {
			Logger.getLogger(DSdream.class).info("Dataset sink writing: " + ds.toString());
			if (!ds.isStreaming()) throw new UnsupportedOperationException("Non-stremaing can't sink");
			DataStreamWriter<V> ss = ds.writeStream().outputMode(OutputMode.Update()).trigger(Trigger.ProcessingTime(500));
			ss = ss.format(OutputSink.FORMAT);
			ss.options(Maps.of("checkpointLocation", "/tmp", "uri", uri.toString()));
			StreamingQuery s = ss.start();
			try {
				s.awaitTermination();
			} catch (StreamingQueryException e) {
				throw new RuntimeException(e);
			}
		}

		// public static <V> void sink(Dataset<V> ds, Consumer<Sdream<V>> using) {
		// Logger.getLogger(DSdream.class).info("Dataset sink writing: " + ds.toString());
		// if (ds.isStreaming()) throw new UnsupportedOperationException("Non-stremaing can't sink");
		// DataStreamWriter<V> ss = ds.writeStream().outputMode(OutputMode.Update()).trigger(Trigger.ProcessingTime(500));
		// ss = ss.format(OutputSink.FORMAT);
		//
		// ss.options(Maps.of("checkpointLocation", "/tmp", "uri", targetUri.toString()));
		// StreamingQuery s = ss.start();
		// try {
		// s.awaitTermination();
		// } catch (StreamingQueryException e) {
		// throw new RuntimeException(e);
		// }
		// }
	}
}
