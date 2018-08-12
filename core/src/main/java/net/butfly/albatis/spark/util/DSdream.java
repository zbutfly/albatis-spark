package net.butfly.albatis.spark.util;

import static net.butfly.albatis.spark.io.SparkIO.$utils$.ENC_R;
import static net.butfly.albatis.spark.io.SparkIO.$utils$.rmap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.impl.WriteHandler;

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

	public static <V> DSdream<V> of(String table, Dataset<Row> impl) {
		Dataset<V> dds = (Dataset<V>) impl.map(row -> rmap(table, row), ENC_R);
		return of(dds);
	}

	public static <V> DSdream<V> of(SQLContext ctx, Sdream<V> s) {
		if (s instanceof DSdream) return (DSdream<V>) s;
		else return of((Dataset<V>) ctx.createDataset((List<Rmap>) s.list(), ENC_R));
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
		return (Sdream<R>) new DSdream<>(ds.map(m -> (Rmap) conv.apply(m), ENC_R));
	}

	@SuppressWarnings("deprecation")
	@Override
	public <R> Sdream<R> map(Function<Sdream<T>, Sdream<R>> conv, int maxBatchSize) {
		return (Sdream<R>) new DSdream<>(ds.flatMap(m -> (Iterator<Rmap>) conv.apply(Sdream.of1(m)).list().iterator(), ENC_R));
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<T, Sdream<R>> flat) {
		return (Sdream<R>) new DSdream<>(ds.flatMap(m -> (Iterator<Rmap>) flat.apply(m).list().iterator(), ENC_R));
	}

	@Override
	public Sdream<T> union(Sdream<T> another) {
		if (another instanceof DSdream) return new DSdream<>(ds.union(((DSdream<T>) another).ds));
		List<Rmap> l = (List<Rmap>) another.list();
		Dataset<Rmap> ads = ds.sqlContext().createDataset(l, ENC_R);
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
		WriteHandler.save((Dataset<Rmap>) ds, (OddOutput<Rmap>) v -> {
			try {
				using.accept((T) v);
				return true;
			} catch (Throwable t) {
				return false;
			}
		});
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
}
