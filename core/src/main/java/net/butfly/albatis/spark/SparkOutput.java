package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Schemas.ENC_RMAP;
import static net.butfly.albatis.spark.impl.Schemas.ROW_KEY_VALUE_FIELD;
import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.Schemas.row2rmap;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO;

public abstract class SparkOutput<V> extends SparkIO implements Output<V> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public URISpec target() {
		return targetUri;
	}

	public abstract void save(Dataset<Row> ds);

	public final void saver(Dataset<Rmap> rs) {
		String table = alias(rs);
		TableDesc td = schema(table);
		if (null == td) {
			Map<String, TableDesc> ss = schemaAll();
			if (ss.size() == 1) {
				td = ss.values().iterator().next();
				logger().warn("Table [" + table + "] not found in schemas, using first: " + td + " and register it.");
				schema(td);
			} else throw new UnsupportedOperationException(
					"Table name variable, and multiple destination tables. Can's guess default table");
		}
		save(rmap2row(td, rs).alias(td.name));

	public Map<String, String> options(String table) {
		return Maps.of();
	}

	@Override
	public void enqueue(Sdream<V> r) {
		throw new UnsupportedOperationException();
	}

	// ============================
	@Override
	public <V0> Output<V0> prior(Function<V0, V> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables()) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public void save(Dataset<Row> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<Rmap> ds = row2rmap(ds0).map(v0 -> (Rmap) conv.apply((V0) v0), ENC_RMAP);
				save(table, rmap2row(schema(table), ds));
			}
		};
	}

	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables()) {
			private static final long serialVersionUID = 5079963400315523098L;

			@Override
			public void save(Dataset<Row> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<Rmap> ds = row2rmap(ds0).flatMap(//
						v0 -> ((Sdream<Rmap>) conv.apply(Sdream.of1((V0) v0))).list().iterator(), ENC_RMAP);
				save(table, rmap2row(schema(table), ds));
			}
		};
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv, int parallelism) {
		return priors(conv);
	}

	@Override
	public <V0> Output<V0> priorFlat(Function<V0, Sdream<V>> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables()) {
			private static final long serialVersionUID = -2887496205884721038L;

			@Override
			public void save(Dataset<Row> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<Rmap> ds = row2rmap(ds0).flatMap(//
						v0 -> ((Sdream<Rmap>) conv.apply((V0) v0)).list().iterator(), ENC_RMAP);
				save(table, rmap2row(schema(table), ds));
			}
		};
	}

	final Map<String, Dataset<Row>> compute(Map<String, Dataset<Rmap>> vals) {
		Map<String, Dataset<Row>> rs = Maps.of();
		vals.forEach((t, vs) -> compute(vs).forEach(//
				(dt, d) -> rs.compute(dt, (dtt, origin) -> null == origin ? d : d.union(origin))));
		return rs;
	}

	private Map<String, Dataset<Row>> compute(Dataset<Rmap> ds) {
		Map<String, Dataset<Row>> r = Maps.of();
		// List<String> keys = ds.groupByKey(Rmap::table, Encoders.STRING()).keys().collectAsList();
		List<String> keys = ds.groupByKey(Rmap::table, Encoders.STRING()).keys().collectAsList();
		keys = new ArrayList<>(keys);
		while (!keys.isEmpty()) {
			String t = keys.remove(0);
			TableDesc tt = schema(t);
			Dataset<Row> tds;
			if (keys.isEmpty()) tds = rmap2row(tt, ds);
			else {
				tds = rmap2row(tt, ds.filter(rr -> t.equals(rr.table())));
				ds = ds.filter(rr -> !t.equals(rr.table()));
			}
			// tds = tds.drop(ROW_TABLE_NAME_FIELD, ROW_KEY_FIELD_FIELD, ROW_KEY_VALUE_FIELD);
			r.put(t, tds.repartition(col(ROW_KEY_VALUE_FIELD)));
		}
		return r;
	}
}
