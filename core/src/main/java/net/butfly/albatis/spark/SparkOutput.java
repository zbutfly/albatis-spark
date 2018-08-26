package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_KEY_VALUE;
import static net.butfly.albatis.spark.impl.Schemas.ENC_RMAP;
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
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO;
import scala.Tuple2;

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

	final List<Tuple2<String, Dataset<Row>>> compute(List<Tuple2<String, Dataset<Rmap>>> vals) {
		return Colls.flat(Colls.list(vals, t -> {
			Dataset<Rmap> ds = t._2;
			List<Tuple2<String, Dataset<Row>>> r = Colls.list();
			List<Tuple2<String, String>> keys = ds.groupByKey(rr -> new Tuple2<>(rr.table(), rr.tableExpr()), //
					Encoders.tuple(Encoders.STRING(), Encoders.STRING())).keys().collectAsList();
			keys = new ArrayList<>(keys);
			while (!keys.isEmpty()) {
				Tuple2<String, String> tn = keys.remove(0);
				TableDesc td = schema(tn._1);
				if (null == td) td = schema(tn._2);
				if (null == td) throw new RuntimeException("Table definition [" + tn + "] not found in schema");
				Dataset<Row> tds;
				if (keys.isEmpty()) tds = rmap2row(td, ds);
				else {
					tds = rmap2row(td, ds.filter(rr -> tn._1.equals(rr.table())));
					ds = ds.filter(rr -> !tn._1.equals(rr.table()));
				}
				r.add(new Tuple2<>(tn._1, tds.repartition(col(FIELD_KEY_VALUE))));
			}
			return r;
		}));
	}
}
