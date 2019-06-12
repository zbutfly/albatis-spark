package net.butfly.albatis.spark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.Albatis;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.SparkThenInput;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.butfly.albacore.utils.collection.Colls.list;
import static net.butfly.albatis.spark.impl.Schemas.*;
import static org.apache.spark.sql.functions.lit;

public abstract class SparkInput<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	final List<Tuple2<String, Dataset<V>>> vals = list();
	final List<Tuple2<String, Dataset<Row>>> rows = list();

	protected SparkInput(SparkSession spark, URISpec targetUri, Object context, TableDesc... table) throws IOException {
		super(spark, targetUri, table);
		switch (mode()) {
		case RMAP:
			List<Tuple2<String, Dataset<V>>> rmaptbls = load(context);
			rmaptbls.forEach(t -> {
				TableDesc td = schemaAll().get(t._1);
				boolean proj = null == td ? false : td.attr(Desc.PROJECT_FROM);
				if (proj) logger().warn("Non-schema dataset does not support field project (as name) now.");
				vals(t._1, limit(t._2));
			});
			return;
		case ROW:
			List<Tuple2<String, Dataset<Row>>> rowtbls = load(context); //TODO add time monitor
			rowtbls.forEach(t -> {
				Dataset<Row> ds = limit(t._2);
				ds = cut(ds, t._1);
				rows(t._1, ds);
			});
			return;
		default:
		}
	}

	private Dataset<Row> cut(Dataset<Row> ds, String table) {
		if (null == table) return ds;
		TableDesc td = schemaAll().get(table);
		if (null == td) return ds;
		if (td.attr(Desc.PROJECT_FROM, false)) {
			for (StructField sf : ds.schema().fields()) {
				boolean cut = true;
				String dbField;
				for (FieldDesc fd : td.fields())
					if (null != (dbField = fd.attr(Desc.PROJECT_FROM)) && dbField.equals(sf.name())) { // rename col
						ds = ds.withColumnRenamed(dbField, fd.name);
						cut = false;
					} else if (fd.name.equals(sf.name())) cut = false; // reserve column
				if (cut) ds = ds.drop(sf.name()); // not in table fields, cut!
			}
			logger().info("Dataset projection into: " + ds.schema().treeString());
		}
		return ds;
	}

	/**
	 * @return Dataset of Rmap or Row, based on data source type (fix schema db like mongodb, or schemaless data like kafka)
	 */
	protected abstract <T> List<Tuple2<String, Dataset<T>>> load(Object context) throws IOException;

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

	public final SparkInput<V> vals(String table, Dataset<V> rmaps) {
		this.vals.add(new Tuple2<>(table, rmaps));
		this.rows.clear();
		return this;
	}

	public final SparkInput<V> rows(String table, Dataset<Row> rows) {
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
		List<Tuple2<String, Dataset<Rmap>>> dss1 = list(vals, t -> new Tuple2<>(t._1, (Dataset<Rmap>) t._2.filter(predicater::test)));
		try {
			return (SparkInput<V>) new SparkThenInput(this, dss1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		List<Tuple2<String, Dataset<Rmap>>> dss1 = list(vals(), t -> {
			Dataset<Rmap> ds1 = t._2.map((MapFunction<V, Rmap>) r -> (Rmap) conv.apply(r), ENC_RMAP);
			return new Tuple2<>(t._1, ds1);
		});
		try {
			return (SparkInput<V1>) new SparkThenInput(this, dss1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <V1> SparkInput<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return thenFlat(v -> conv.apply(Sdream.of1(v)));
	}

	@Deprecated
	@Override
	public <V1> SparkInput<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return thens(conv);
	}

	/*
	 * destination class should be Rmap or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> SparkInput<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		List<Tuple2<String, Dataset<Rmap>>> dss1 = list(vals(), t -> {
			Dataset<Rmap> ds1 = t._2.flatMap((FlatMapFunction<V, Rmap>) v -> ((List<Rmap>) conv.apply(v).list()).iterator(), ENC_RMAP);
			return new Tuple2<>(t._1, ds1);
		});
		try {
			return (SparkInput<V1>) new SparkThenInput(this, dss1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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

    // give SparkInput,return Limited it
	public SparkInput limitResult(int outMaxCount){
		List<Tuple2<String, Dataset<Row>>> originalList = this.rows();
		if (originalList.size() == 1){
			Dataset<Row> rowDataset = originalList.get(0)._2; //TODO  foreach it,list might has more ds
			Dataset<Row> limitedDS = rowDataset.limit(outMaxCount);
			String tableName = this.rows.get(0)._1; //TODO to foreach
			this.rows.clear(); //should clear old rows;
			this.rows(tableName,limitedDS); //TODO should has add more ds method; give it list
			return this;
		}else{
			throw new IllegalArgumentException("limitResult only support one ds one sparkInput present");
		}
	}

	public SparkInput<V> with(String colname, Object value) {
		List<Tuple2<String, Dataset<Row>>> originrows = new ArrayList<>(rows());
		rows.clear();
		vals.clear();
		originrows.forEach(t -> rows.add(new Tuple2<>(t._1, t._2.withColumn(colname, lit(value)))));
		originrows.clear();
		return this;
	}

	public SparkInput<V> alias(String dsttbl) {
		List<Tuple2<String, Dataset<Row>>> originrows = new ArrayList<>(rows());
		rows.clear();
		vals.clear();
		originrows.forEach(t -> rows.add(new Tuple2<>(dsttbl, t._2)));
		originrows.clear();
		return this;
	}

	public String infoPlan() {
		List<String> plans = Colls.list(rows(), t -> t._2.logicalPlan().prettyJson());
		return String.join("\n", plans);
	}

	public String infoSchema() {
		List<String> schemas = Colls.list(rows(), t -> t._1 + ":" + t._2.schema().treeString());
		return String.join("\n", schemas);
	}

	public void infoData() {
		rows.forEach(t -> t._2.show(true));
	}
}
