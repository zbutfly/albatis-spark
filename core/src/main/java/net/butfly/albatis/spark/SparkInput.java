package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.byTable;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.row2rmap;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.rmap2row;

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
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.SparkInputWrapper;
import net.butfly.albatis.spark.impl.Sparks;

public abstract class SparkInput<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	private Map<String, Dataset<V>> vals = Maps.of();
	private Map<String, Dataset<Row>> rows = Maps.of();

	protected SparkInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		Dataset<Row> ds = load();
		if (null != ds && Systems.isDebug()) {
			int limit = Integer.parseInt(Configs.gets("albatis.spark.debug.limit", "-1"));
			if (limit > 0) {
				ds = ds.limit(limit);
				long n = ds.count();
				logger().error("Debugging, resultset is limit as [" + limit + "] by setting \"albatis.spark.debug.limit\","//
						+ " results count: " + n);
			} else logger().info(
					"Debugging, resultset can be limited as setting \"albatis.spark.debug.limit\", if presented and positive.");
		}
		rows(Maps.of("*", ds));
	}

	public Map<String, String> options() {
		return Maps.of();
	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<V>> vals() {
		if (!vals.isEmpty()) return vals;
		Map<String, Dataset<V>> r = Maps.of();
		if (rows.containsKey("*")) {
			Map<String, Dataset<Row>> dss = byTable(rows.remove("*"));
			for (String t : dss.keySet()) {
				rows.put(t, dss.get(t));
				r.put(t, (Dataset<V>) row2rmap(dss.get(t)));
			}
		} else for (String t : rows.keySet())
			r.put(t, (Dataset<V>) row2rmap(rows.get(t)));
		return r;

	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<Row>> rows() {
		if (!rows.isEmpty()) return rows;
		else {
			Map<String, Dataset<V>> dss = vals();
			Map<String, Dataset<Row>> dsr = Maps.of();
			for (String t : dss.keySet()) {
				Dataset<Row> ds = rmap2row(schema(t), (Dataset<Rmap>) dss.get(t));
				dsr.put(t, ds);
			}
			return dsr;
		}
	}

	@SuppressWarnings("unchecked")
	public final Map<String, Dataset<Row>> rowsOut(Map<String, TableDesc> schemas) {
		if (!rows.isEmpty()) return rows;
		else {
			Map<String, Dataset<V>> dss = vals();
			Map<String, Dataset<Row>> dsr = Maps.of();
			for (String t : dss.keySet()) {
				Map<String, Dataset<Row>> dss1 = byTable((Dataset<Rmap>) dss.get(t), schemas);
				dss1.forEach((dt, d) -> dsr.compute(dt, (dtt, origin) -> {
					if (null == origin) return d;
					else return d.union(origin);
				}));
			}
			return dsr;
		}
	}

	protected final SparkInput<V> vals(Map<String, Dataset<V>> rmaps) {
		this.vals.putAll(rmaps);
		this.rows.clear();
		return this;
	}

	protected final SparkInput<V> rows(Map<String, Dataset<Row>> rows) {
		this.rows.putAll(rows);;
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

	protected abstract Dataset<Row> load();

	@Override
	public void close() {
		OddInput.super.close();
	}

	// ---------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public SparkInput<V> filter(Predicate<V> predicater) {
		Map<String, Dataset<V>> dss = vals();
		Map<String, Dataset<Rmap>> dss1 = Maps.of();
		for (String t : dss.keySet())
			dss1.put(t, (Dataset<Rmap>) dss.get(t).filter(predicater::test));
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
		Map<String, Dataset<V>> dss = vals();
		Map<String, Dataset<Rmap>> dss1 = Maps.of();
		for (String t : dss.keySet())
			dss1.put(t, dss.get(t).map(r -> (Rmap) conv.apply(r), Sparks.ENC_RMAP));
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
		Map<String, Dataset<V>> dss = vals();
		Map<String, Dataset<Rmap>> dss1 = Maps.of();
		for (String t : dss.keySet())
			dss1.put(t, dss.get(t).flatMap(v -> ((List<Rmap>) conv.apply(v).list()).iterator(), Sparks.ENC_RMAP));
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
