package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.byTable;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.row2rmap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.impl.Sparks;

public abstract class SparkOutput<V> extends SparkIO implements Output<V> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public URISpec target() {
		return targetUri;
	}

	public abstract void save(Dataset<Row> dataset);

	private final void saveRmap(Dataset<Rmap> ds) {
		byTable(ds, schemaAll(), (t, d) -> SparkOutput.this.save(d));
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
				Dataset<Rmap> ds = row2rmap(ds0).map(v0 -> (Rmap) conv.apply((V0) v0), Sparks.ENC_RMAP);
				saveRmap(ds);
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
						v0 -> ((Sdream<Rmap>) conv.apply(Sdream.of1((V0) v0))).list().iterator(), Sparks.ENC_RMAP);
				saveRmap(ds);
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
						v0 -> ((Sdream<Rmap>) conv.apply((V0) v0)).list().iterator(), Sparks.ENC_RMAP);
				saveRmap(ds);
			}
		};
	}
}
