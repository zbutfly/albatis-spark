package net.butfly.albatis.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO;

public abstract class SparkOutput<V> extends SparkIO implements Output<V> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public URISpec target() {
		return targetUri;
	}

	public abstract void save(Dataset<V> ds);

	@Override
	public void enqueue(Sdream<V> r) {
		throw new UnsupportedOperationException();
	}

	// ============================
	@Override
	public <V0> Output<V0> prior(Function<V0, V> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public void save(Dataset<V0> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<V> ds = (Dataset<V>) ds0.map(v0 -> (Rmap) conv.apply(v0), $utils$.ENC_R);
				SparkOutput.this.save(ds);
			}
		};
	}

	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables) {
			private static final long serialVersionUID = 5079963400315523098L;

			@Override
			public void save(Dataset<V0> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<V> ds = (Dataset<V>) ds0.flatMap(v0 -> ((Sdream<Rmap>) conv.apply(Sdream.of(v0))).list().iterator(), $utils$.ENC_R);
				SparkOutput.this.save(ds);
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
		return new SparkOutput<V0>(spark, targetUri, tables) {
			private static final long serialVersionUID = -2887496205884721038L;

			@Override
			public void save(Dataset<V0> ds0) {
				@SuppressWarnings("unchecked")
				Dataset<V> ds = (Dataset<V>) ds0.flatMap(v0 -> ((Sdream<Rmap>) conv.apply(v0)).list().iterator(), $utils$.ENC_R);
				SparkOutput.this.save(ds);
			}
		};
	}
}
