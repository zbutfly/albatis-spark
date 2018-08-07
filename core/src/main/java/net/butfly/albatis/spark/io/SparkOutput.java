package net.butfly.albatis.spark.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.util.DSdream;
import net.butfly.albatis.spark.util.DSdream.$utils$;

@SuppressWarnings("unchecked")
public abstract class SparkOutput extends SparkIO implements Output<Rmap>, SparkOutputWriter<Rmap> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		Dataset<Rmap> ds = s instanceof DSdream ? ((DSdream<Rmap>) s).ds : spark.sqlContext().createDataset(s.list(), $utils$.ENC_R);
		streaming = saving(ds, this);
	}

	// ============================
	@Override
	public <V0> Output<V0> prior(Function<V0, Rmap> conv) {
		return (Output<V0>) new SparkOutput(spark, targetUri, tables) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public void process(Rmap r) {
				SparkOutput.this.process(conv.apply((V0) r));
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkOutput.this.enqueue((DSdream<Rmap>) s.map(r -> conv.apply((V0) r)));
			}
		};
	}

	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<Rmap>> conv) {
		return (Output<V0>) new SparkOutput(spark, targetUri, tables) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public void process(Rmap r) {
				conv.apply(Sdream.of((V0) r)).eachs(rr -> SparkOutput.this.process((Rmap) rr));
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkOutput.this.enqueue((DSdream<Rmap>) conv.apply((Sdream<V0>) s));
			}
		};
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<Rmap>> conv, int parallelism) {
		return priors(conv);
	}

	@Override
	public <V0> Output<V0> priorFlat(Function<V0, Sdream<Rmap>> conv) {
		return (Output<V0>) new SparkOutput(spark, targetUri, tables) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public void process(Rmap r) {
				conv.apply((V0) r).eachs(rr -> SparkOutput.this.process((Rmap) rr));
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkOutput.this.enqueue((DSdream<Rmap>) s.mapFlat(r -> conv.apply((V0) r)));
			}
		};
	}
}
