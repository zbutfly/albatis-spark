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
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
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
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

public final class DSdream implements Sdream<Rmap>/* , Dataset<Rmap> */ {
	private static final long serialVersionUID = 4996999561898013014L;
	protected final Dataset<Rmap> ds;

	public DSdream(Dataset<Rmap> impl) {
		ds = impl;
	}

	@Override
	public Spliterator<Rmap> spliterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Sdream<Rmap> ex(Exeter ex) {
		return this;
	}

	// =======================================

	@Override
	public Rmap reduce(BinaryOperator<Rmap> accumulator) {
		return ds.reduce(accumulator::apply);
	}

	// conving =======================================
	@Override
	public Sdream<Rmap> filter(Predicate<Rmap> checking) {
		return new DSdream(ds.filter(checking::test));
	}

	@Override
	@Deprecated
	public Sdream<Sdream<Rmap>> batch(int maxBatchSize) {
		// List<Sdream<Rmap>> dss = Colls.list();
		// double[] weights = new double[] {};
		// for (Dataset<Rmap> d : ds.randomSplit(weights)) {
		// dss.add(new DSdream(d));
		// }
		// return Sdream.of(dss);
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R> Sdream<R> map(Function<Rmap, R> conv) {
		return (Sdream<R>) new DSdream(ds.map(m -> (Rmap) conv.apply(m), $utils$.ENC_R));
	}

	@SuppressWarnings("unchecked")
	@Override
	@Deprecated
	public <R> Sdream<R> map(Function<Sdream<Rmap>, Sdream<R>> conv, int maxBatchSize) {
		return (Sdream<R>) new DSdream(ds.flatMap(m -> (Iterator<Rmap>) conv.apply(Sdream.of(Colls.list(m))).list().iterator(),
				$utils$.ENC_R));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R> Sdream<R> mapFlat(Function<Rmap, Sdream<R>> flat) {
		return (Sdream<R>) new DSdream(ds.flatMap(m -> (Iterator<Rmap>) flat.apply(m).list().iterator(), $utils$.ENC_R));
	}

	@Override
	public Sdream<Rmap> union(Sdream<Rmap> another) {
		return another instanceof DSdream ? new DSdream(ds.union(((DSdream) another).ds))
				: new DSdream(ds.union(ds.sqlContext().createDataset(another.list(), $utils$.ENC_R)));
	}

	@Override
	public <E1> Sdream<Pair<Rmap, E1>> join(Function<Sdream<Rmap>, Sdream<E1>> joining, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<Rmap> using) {
		StreamingQuery s = $utils$.foreach(ds, using);
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
	public void each(Consumer<Rmap> using) {
		eachs(using);
	}

	@Override
	public void batch(Consumer<Sdream<Rmap>> using, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void partition(Consumer<Sdream<Rmap>> using, int minPartNum) {
		partition(minPartNum).forEach(using::accept);
	}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<Rmap>> using, Function<Rmap, K> keying, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Sdream<Rmap>> partition(int minPartNum) {
		List<Sdream<Rmap>> dss = Colls.list();
		double[] weights = new double[] {};
		for (Dataset<Rmap> d : ds.randomSplit(weights))
			dss.add(new DSdream(d));
		return dss;
	}

	@Override
	public <K> void partition(BiConsumer<K, Rmap> using, Function<Rmap, K> keying) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<Rmap, K> keying, Function<Rmap, V> valuing) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> Map<K, V> partition(Function<Rmap, K> keying, Function<Rmap, V> valuing, BinaryOperator<V> reducing) {
		throw new UnsupportedOperationException();
	}

	// internal =============================

	@Override
	public Optional<Rmap> next() {
		throw new UnsupportedOperationException();
	}

	public static interface $utils$ extends Serializable {
		static <V> StreamingQuery foreach(Dataset<V> ds, Consumer<V> using) {
			if (ds.isStreaming()) {
				ForeachWriter<V> w = new ForeachWriter<V>() {
					private static final long serialVersionUID = 3602739322755312373L;

					@Override
					public void process(V r) {
						using.accept(r);
					}

					@Override
					public boolean open(long partitionId, long version) {
						return true;
					}

					@Override
					public void close(Throwable err) {}
				};
				DataStreamWriter<V> s = ds.writeStream();
				s = s.foreach(w);
				s = s.outputMode(OutputMode.Update());
				s = s.trigger(Trigger.ProcessingTime(500));
				return s.start();
			} else {
				ds.foreach(using::accept);
				return null;
			}
		}

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

		public static Rmap rMap(Row row) {
			Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala()
					.toSeq();
			Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
			String t = (String) map.remove("___table");
			if (null != t) return new Rmap(t, map);
			else return new Rmap(map);
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

		public static scala.collection.Map<String, Object> mapizeScala(java.util.Map<String, Object> value) {
			return scala.collection.JavaConversions.mapAsScalaMap(value);
		}

		public static java.util.Map<String, Object> mapizeJava(scala.collection.Map<String, Object> vs) {
			return scala.collection.JavaConversions.mapAsJavaMap(vs);
		}
	}
}
