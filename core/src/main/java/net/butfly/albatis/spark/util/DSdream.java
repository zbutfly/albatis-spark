package net.butfly.albatis.spark.util;

import static net.butfly.albatis.spark.impl.Schemas.ENC_RMAP;
import static net.butfly.albatis.spark.impl.Schemas.build;
import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.Schemas.row2rmap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
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
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.output.SparkSinkOutput;

@SuppressWarnings("unchecked")
public final class DSdream implements Sdream<Rmap>/* , Dataset<T> */ {
	private static final long serialVersionUID = 4996999561898013014L;
	public final Dataset<Row> ds;

	private DSdream(Dataset<Row> impl) {
		ds = impl;
	}

	public static DSdream of(Dataset<Row> impl) {
		return new DSdream(impl);
	}

	public static DSdream ofMap(Dataset<Rmap> ds, TableDesc schema) {
		return new DSdream(rmap2row(schema, ds).alias(schema.name));

	}

	public static DSdream of(SQLContext ctx, Sdream<Rmap> s, TableDesc schema) {
		if (s instanceof DSdream) return new DSdream(((DSdream) s).ds);
		Dataset<Rmap> rds = ctx.createDataset(s.list(), ENC_RMAP);
		return of(rmap2row(schema, rds).alias(schema.name));
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
		return row2rmap(ds).reduce((ReduceFunction<Rmap>) accumulator::apply);
	}

	// conving =======================================
	@Override
	public Sdream<Rmap> filter(Predicate<Rmap> checking) {
		Dataset<Rmap> dds = row2rmap(ds).filter(checking::test);
		return ofMap(dds, build(ds.schema()));
	}

	@Override
	@Deprecated
	public Sdream<Sdream<Rmap>> batch(int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R> Sdream<R> map(Function<Rmap, R> conv) {
		Dataset<Rmap> dds = row2rmap(ds).map((MapFunction<Rmap, Rmap>)  m -> (Rmap) conv.apply(m), ENC_RMAP);
		return (Sdream<R>) ofMap(dds, build(ds.schema()));
	}

	@SuppressWarnings("deprecation")
	@Override
	public <R> Sdream<R> map(Function<Sdream<Rmap>, Sdream<R>> conv, int maxBatchSize) {
		Dataset<Rmap> dds = row2rmap(ds).flatMap((FlatMapFunction<Rmap, Rmap>)  m -> (Iterator<Rmap>) conv.apply(Sdream.of1(m)).list().iterator(), ENC_RMAP);
		return (Sdream<R>) ofMap(dds, build(ds.schema()));
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<Rmap, Sdream<R>> flat) {
		Dataset<Rmap> dds = row2rmap(ds).flatMap((FlatMapFunction<Rmap, Rmap>) m -> (Iterator<Rmap>) flat.apply(m).list().iterator(), ENC_RMAP);
		return (Sdream<R>) ofMap(dds, build(ds.schema()));
	}

	@Override
	public Sdream<Rmap> union(Sdream<Rmap> another) {
		if (another instanceof DSdream) return new DSdream(ds.union(((DSdream) another).ds).alias(alias(ds)));
		Dataset<Rmap> ads = ds.sqlContext().createDataset(another.list(), ENC_RMAP);
		Dataset<Rmap> uds = row2rmap(ds).union(ads);
		return ofMap(uds, build(ds.schema()));// TODO: merge schema
	}

	@Override
	public <E> Sdream<Pair<Rmap, E>> join(Function<Sdream<Rmap>, Sdream<E>> joining, int maxBatchSize) {
		throw new UnsupportedOperationException();
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<Rmap> using) {
		try (SparkSinkOutput o = new SparkSinkOutput(ds.sparkSession(), (Consumer<Rmap>) using);) {
			o.save(ds);
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
		for (Dataset<Row> d : ds.randomSplit(weights))
			dss.add(new DSdream(d.alias(alias(ds))));
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
}
