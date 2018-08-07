package net.butfly.albatis.spark.util;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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
import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkOutput;
import net.butfly.albatis.spark.io.SparkOutputWriter;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

@SuppressWarnings("unchecked")
public final class DSdream<T> implements Sdream<T>/* , Dataset<T> */ {
	private static final long serialVersionUID = 4996999561898013014L;
	public final Dataset<T> ds;

	private DSdream(Dataset<T> impl) {
		ds = impl;
	}

	public static <T> DSdream<T> of(Dataset<T> impl) {
		return new DSdream<>(impl);
	}

	public static DSdream<Rmap> of(String table, Dataset<Row> impl) {
		return new DSdream<>(impl.map(row -> $utils$.rmap(table, row), $utils$.ENC_R));
	}

	public static DSdream<Rmap> of(SQLContext ctx, Sdream<Rmap> s) {
		return s instanceof DSdream ? (DSdream<Rmap>) s : of(ctx.createDataset(s.list(), $utils$.ENC_R));
	}

	@Override
	public Spliterator<T> spliterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Sdream<T> ex(Exeter ex) {
		return this;
	}

	// =======================================

	@Override
	public T reduce(BinaryOperator<T> accumulator) {
		return ds.reduce(accumulator::apply);
	}

	// conving =======================================
	@Override
	public Sdream<T> filter(Predicate<T> checking) {
		return new DSdream<>(ds.filter(checking::test));
	}

	@Override
	@Deprecated
	public Sdream<Sdream<T>> batch(int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R> Sdream<R> map(Function<T, R> conv) {
		return (Sdream<R>) new DSdream<>(ds.map(m -> (Rmap) conv.apply(m), $utils$.ENC_R));
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public <R> Sdream<R> map(Function<Sdream<T>, Sdream<R>> conv, int maxBatchSize) {
		return (Sdream<R>) new DSdream<>(ds.flatMap(m -> (Iterator<Rmap>) conv.apply(Sdream.of(Colls.list(m))).list().iterator(),
				$utils$.ENC_R));
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<T, Sdream<R>> flat) {
		return (Sdream<R>) new DSdream<>(ds.flatMap(m -> (Iterator<Rmap>) flat.apply(m).list().iterator(), $utils$.ENC_R));
	}

	@Override
	public Sdream<T> union(Sdream<T> another) {
		if (another instanceof DSdream) return new DSdream<>(ds.union(((DSdream<T>) another).ds));
		List<Rmap> l = (List<Rmap>) another.list();
		Dataset<Rmap> ads = ds.sqlContext().createDataset(l, $utils$.ENC_R);
		Dataset<Rmap> uds = ((Dataset<Rmap>) ds).union(ads);
		return (Sdream<T>) new DSdream<>(uds);
	}

	@Override
	public <E1> Sdream<Pair<T, E1>> join(Function<Sdream<T>, Sdream<E1>> joining, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<T> using) {
		StreamingQuery s = saving(using::accept, null, Maps.of());
		if (null != s) try {
			s.awaitTermination();
		} catch (StreamingQueryException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Using spliterator parallelly with trySplit()
	 * 
	 * @return
	 */
	@Override
	public void each(Consumer<T> using) {
		eachs(using);
	}

	@Override
	public void batch(Consumer<Sdream<T>> using, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void partition(Consumer<Sdream<T>> using, int minPartNum) {
		partition(minPartNum).forEach(using::accept);
	}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<T>> using, Function<T, K> keying, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Sdream<T>> partition(int minPartNum) {
		List<Sdream<T>> dss = Colls.list();
		double[] weights = new double[] {};
		for (Dataset<T> d : ds.randomSplit(weights))
			dss.add(new DSdream<>(d));
		return dss;
	}

	@Override
	public <K> void partition(BiConsumer<K, T> using, Function<T, K> keying) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<T, K> keying, Function<T, V> valuing) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> Map<K, V> partition(Function<T, K> keying, Function<T, V> valuing, BinaryOperator<V> reducing) {
		throw new UnsupportedOperationException();
	}

	// internal =============================

	@Override
	public Optional<T> next() {
		throw new UnsupportedOperationException();
	}

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
	}

	public StreamingQuery saving(SparkOutputWriter<T> writer, String format, Map<String, String> opts) {
		Logger.getLogger(DSdream.class).info("Dataset writing: " + ds.toString());
		if (ds.isStreaming()) {
			DataStreamWriter<T> ss = ds.writeStream().outputMode(OutputMode.Update()).trigger(Trigger.ProcessingTime(500));
			if (null != format) ss = ss.format(format);
			ss = null != writer ? ss.foreach(new SparkOutputWriter.Writer<>((SparkOutput) writer))
					: ss.option("checkpointLocation", "/tmp");// TODO
			if (!opts.isEmpty()) ss.options(opts);
			return ss.start();
		} else {
			ds.foreach(writer::process);
			return null;
		}
	}
}
