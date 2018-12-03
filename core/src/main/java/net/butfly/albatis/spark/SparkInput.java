package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Schemas.ENC_RMAP;
import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.Schemas.row2rmap;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.SparkThenInput;
import scala.Tuple2;

public abstract class SparkInput<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	final List<Tuple2<String, Dataset<V>>> vals = Colls.list();
	final List<Tuple2<String, Dataset<Row>>> rows = Colls.list();

	protected SparkInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		switch (mode()) {
		case RMAP:
			List<Tuple2<String, Dataset<V>>> ds = load();
			ds.forEach(t -> vals(t._1, limit(t._2)));
			return;
		case ROW:
			List<Tuple2<String, Dataset<Row>>> rs = load();
			rs.forEach(t -> rows(t._1, limit(t._2)));
			return;
		default:
		}
	}


	/**
	 * @return Dataset of Rmap or Row, based on data source type (fix schema db like mongodb, or schemaless data like kafka)
	 */
	protected abstract <T> List<Tuple2<String, Dataset<T>>> load();

	public enum DatasetMode {
		NONE, RMAP, ROW
	}

	protected DatasetMode mode() {
		return DatasetMode.NONE;
	}

	public Map<String, String> options() {
		return Maps.of();
	}

	@SuppressWarnings("unchecked")
	public final List<Tuple2<String, Dataset<V>>> vals() {
		if (vals.isEmpty()) rows.forEach(t -> vals(t._1, (Dataset<V>) row2rmap(t._2)));
		return vals;
	}

	@SuppressWarnings("unchecked")
	public final List<Tuple2<String, Dataset<Row>>> rows() {
		if (rows.isEmpty()) vals.forEach(t -> rows(t._1, rmap2row(schema(t._1), (Dataset<Rmap>) t._2)));
		return rows;
	}

	protected final SparkInput<V> vals(String table, Dataset<V> rmaps) {
		this.vals.add(new Tuple2<>(table, rmaps));
		this.rows.clear();
		return this;
	}

	protected final SparkInput<V> rows(String table, Dataset<Row> rows) {
		this.rows.add(new Tuple2<>(table, rows));
		this.vals.clear();
		return this;
	}

	@Override
	public final V dequeue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deq(Consumer<V> using) {
		throw new UnsupportedOperationException("SparkInput can be pump to output only");
		// if (dataset.isStreaming()) sink(dataset, rmaps -> rmaps.eachs(using));
		// else each(dataset, using::accept);
	}

	@Override
	public final void dequeue(Consumer<Sdream<V>> using) {
		throw new UnsupportedOperationException("SparkInput can be pump to output only");
		// if (dataset.isStreaming()) sink(dataset, using);
		// else each(dataset, r -> using.accept(Sdream.of1(r)));
	}

	@Override
	public void close() {
		OddInput.super.close();
	}

	// ---------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public SparkInput<V> filter(Predicate<V> predicater) {
		List<Tuple2<String, Dataset<Rmap>>> dss1 = Colls.list(vals, t -> new Tuple2<>(t._1, (Dataset<Rmap>) t._2.filter(predicater::test)));
		return (SparkInput<V>) new SparkThenInput(this, dss1);
	}

	@Override
	@Deprecated
	public SparkInput<V> filter(Map<String, Object> criteria) {
		throw new UnsupportedOperationException();
	}

	/*
	 * destination class should be Rmap or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> SparkInput<V1> then(Function<V, V1> conv) {
		List<Tuple2<String, Dataset<Rmap>>> dss1 = Colls.list(vals(), t -> {
			Dataset<Rmap> ds1 = t._2.map((MapFunction<V, Rmap>)  r -> (Rmap) conv.apply(r), ENC_RMAP);
			return new Tuple2<>(t._1, ds1);
		});
		return (SparkInput<V1>) new SparkThenInput(this, dss1);
	}

	@Override
	public <V1> SparkInput<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return thenFlat(v -> conv.apply(Sdream.of1(v)));
	}

	@Override
	@SuppressWarnings("deprecation")
	public <V1> SparkInput<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return thens(conv);
	}

	/*
	 * destination class should be Rmap or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> SparkInput<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		List<Tuple2<String, Dataset<Rmap>>> dss1 = Colls.list(vals(), t -> {
			Dataset<Rmap> ds1 = t._2.flatMap((FlatMapFunction<V, Rmap>)  v -> ((List<Rmap>) conv.apply(v).list()).iterator(), ENC_RMAP);
			return new Tuple2<>(t._1, ds1);
		});
		return (SparkInput<V1>) new SparkThenInput(this, dss1);
	}




	@SuppressWarnings("unchecked")
	@Override
	public Pump<V> pump(int parallelism, Output<V> dest) {
		return (Pump<V>) pump((Output<Rmap>) dest);
	}

	@SuppressWarnings("unchecked")
	public SparkPump pump(Output<Rmap> output) {
		return new SparkPump((SparkInput<Rmap>) this, output);
	}

	@Override
	public int features() {
		int f = super.features();
		// if (vals().isStreaming()) f |= IO.Feature.STREAMING;
		return f;
	}

	private <T> Dataset<T> limit(Dataset<T> ds) {
		if (null != ds && Systems.isDebug()) {
			@SuppressWarnings("deprecation")
			int limit = Integer.parseInt(Configs.gets(Albatis.PROP_DEBUG_INPUT_LIMIT, "-1"));
			if (limit > 0) {
				ds = ds.limit(limit);
				long n = ds.count();
				logger().error("Debugging, resultset is limit as [" + limit + "] by setting \"" + Albatis.PROP_DEBUG_INPUT_LIMIT + "\","//
						+ " results count: " + n);
			} else logger().info("Debugging, resultset can be limited as setting \"" + Albatis.PROP_DEBUG_INPUT_LIMIT
					+ "\", if presented and positive.");
		}
		return ds;
	}
}
