package net.butfly.albatis.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Wrapper;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.Sparks;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.*;

public abstract class SparkInput<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	private Dataset<V> vals;
	private Dataset<Row> rows;

	protected SparkInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		rows(load());
		if (null != vals && Systems.isDebug()) {
			int limit = Integer.parseInt(Configs.gets("albatis.spark.debug.limit", "-1"));
			if (limit > 0) {
				rows(rows.limit(limit));
				long n = rows.count();
				logger().error("Debugging, resultset is limit as [" + limit + "] by setting \"albatis.spark.debug.limit\","//
						+ " results count: " + n);
			} else logger().info(
					"Debugging, resultset can be limited as setting \"albatis.spark.debug.limit\", if presented and positive.");
		}
	}

	public final Dataset<V> vals() {
		return vals;
	}

	public final Dataset<Row> rows() {
		return rows;
	}

	@SuppressWarnings("unchecked")
	protected final SparkInput<V> vals(Dataset<V> rmaps) {
		this.vals = rmaps;
		rows = null != vals && schemaMode() == SchemaMode.SINGLE ? rmap2row(table(), (Dataset<Rmap>) vals) : null;
		return this;
	}

	@SuppressWarnings("unchecked")
	protected final SparkInput<V> rows(Dataset<Row> rows) {
		this.rows = rows;
		this.vals = null == rows ? null : (Dataset<V>) row2rmap(rows);
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

	protected abstract Dataset<Row> load();

	@Override
	public void close() {
		OddInput.super.close();
	}

	// ---------------------------------------------------------------------
	private static class SparkInputWrapper<VV> extends SparkInput<VV> implements Wrapper<SparkInput<VV>> {
		private static final long serialVersionUID = 5957738224117308018L;
		private final SparkInput<?> base;

		protected SparkInputWrapper(SparkInput<?> s, Dataset<VV> ds) {
			super(s.spark, s.targetUri);
			this.base = s;
			vals(ds);
		}

		@Override
		protected Dataset<Row> load() {
			return null;
		}

		@Override
		public Map<String, String> options() {
			return base.options();
		}

		@Override
		public <BB extends IO> BB bases() {
			return Wrapper.bases(base);
		}
	}

	@Override
	public SparkInput<V> filter(Predicate<V> predicater) {
		return new SparkInputWrapper<>(this, vals.filter(predicater::test));
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
		return new SparkInputWrapper<>(this, (Dataset<V1>) vals.map(r -> (Rmap) conv.apply(r), Sparks.ENC_RMAP));
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
		return new SparkInputWrapper<>(this, (Dataset<V1>) vals.flatMap(v -> ((List<Rmap>) conv.apply(v).list()).iterator(),
				Sparks.ENC_RMAP));
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
		if (vals.isStreaming()) f |= IO.Feature.STREAMING;
		return f;
	}
}
