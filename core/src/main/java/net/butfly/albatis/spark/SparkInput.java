package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Schemas.compute;
import static net.butfly.albatis.spark.impl.Schemas.row2rmap;
import static net.butfly.albatis.spark.impl.Schemas.rmap2row;

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
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.SparkInputWrapper;

public abstract class SparkInput<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	private List<Dataset<V>> vals = Colls.list();
	private List<Dataset<Row>> rows = Colls.list();

	protected SparkInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		List<Dataset<Row>> ds = load();
		if (null != ds && !ds.isEmpty()) {
			if (Systems.isDebug()) {
				@SuppressWarnings("deprecation")
				int limit = Integer.parseInt(Configs.gets("albatis.spark.debug.limit", "-1"));
				if (limit <= 0) logger().info(
						"Debugging, resultset can be limited as setting \"albatis.spark.debug.limit\", if presented and positive.");
				else ds = Colls.list(ds, d -> {
					Dataset<Row> dd = d.limit(limit).alias(alias(d));
					long n = dd.count();
					logger().error("Debugging, resultset is limit as [" + limit + "] by setting \"albatis.spark.debug.limit\","//
							+ " results count: " + n);
					return dd;
				});
			}
			rows(ds);
		}
	}

	public Map<String, String> options() {
		return Maps.of();
	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<V>> vals() {
		if (vals.isEmpty()) {
			Map<String, Dataset<V>> r = Maps.of();
			for (String t : rows.keySet())
				r.put(t, (Dataset<V>) row2rmap(rows.get(t)));
			vals(r);
		}
		return vals;

	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<Row>> rows() {
		if (rows.isEmpty()) {
			Map<String, Dataset<Row>> dsr = Maps.of();
			for (String t : vals.keySet()) {
				Dataset<Row> ds = rmap2row(schema(t), (Dataset<Rmap>) vals.get(t));
				dsr.put(t, ds);
			}
			rows(dsr);
		}
		return rows;
	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<Row>> rowsOut(Output<Rmap> output) {
		if (!rows.isEmpty()) return rows;
		else {
			Map<String, Dataset<V>> dss = vals();
			Map<String, Dataset<Row>> dsr = Maps.of();
			for (String t : dss.keySet()) {
				Map<String, Dataset<Row>> dss1 = compute((Dataset<Rmap>) dss.get(t), output);
				dss1.forEach((dt, d) -> dsr.compute(dt, (dtt, origin) -> {
					if (null == origin) return d;
					else return d.union(origin);
				}));
			}
			return dsr;
		}
	}

	protected final SparkInput<V> vals(Collection<Dataset<V>> vals) {
		this.vals.clear();
		this.vals.addAll(vals);
		this.rows.clear();
		return this;
	}

	protected final SparkInput<V> rows(Collection<Dataset<Row>> rows) {
		this.rows.clear();
		this.rows.addAll(rows);
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

	protected abstract List<Dataset<Row>> load();

	@Override
	public void close() {
		OddInput.super.close();
	}

	// ---------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public SparkInput<V> filter(Predicate<V> predicater) {
		List<Dataset<V>> dss = vals();
		List<Dataset<Rmap>> dss1 = Colls.list();
		for (Dataset<V> d : dss)
			dss1.add((Dataset<Rmap>) d.filter(predicater::test).alias(alias(d)));
		return (SparkInput<V>) new SparkInputWrapper(this, dss1);
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
		List<Dataset<V>> dss = vals();
		List<Dataset<Rmap>> dss1 = Colls.list();
		for (Dataset<V> d : dss)
			dss1.add(d.map(r -> (Rmap) conv.apply(r), ENC_RMAP).alias(alias(d)));
		return (SparkInput<V1>) new SparkInputWrapper(this, dss1);
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
		List<Dataset<V>> dss = vals();
		List<Dataset<Rmap>> dss1 = Colls.list();
		for (Dataset<V> d : dss)
			dss1.add(d.flatMap(v -> ((List<Rmap>) conv.apply(v).list()).iterator(), ENC_RMAP).alias(alias(d)));
		return (SparkInput<V1>) new SparkInputWrapper(this, dss1);
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
}
