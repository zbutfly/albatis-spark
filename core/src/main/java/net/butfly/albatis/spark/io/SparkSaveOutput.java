package net.butfly.albatis.spark.io;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.util.DSdream;

public abstract class SparkSaveOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = 1509137417654908221L;

	protected SparkSaveOutput(SparkSession spark, URISpec targetUri, String table) {
		super(spark, targetUri, table);
	}

	// =====
	@SuppressWarnings("unchecked")
	@Override
	public <V0> Output<V0> prior(Function<V0, Rmap> conv) {
		return (Output<V0>) new SparkSaveOutput(spark, targetUri, table()) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public boolean enqueue(Rmap r) {
				if (null == r) return false;
				return SparkSaveOutput.this.enqueue(conv.apply((V0) r));
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkSaveOutput.this.enqueue((DSdream<Rmap>) s.map(r -> conv.apply((V0) r)));
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<Rmap>> conv) {
		return (Output<V0>) new SparkSaveOutput(spark, targetUri, table()) {
			private static final long serialVersionUID = 5079963400315523098L;

			@Override
			public boolean enqueue(Rmap r) {
				if (null == r) return false;
				enqueue(Sdream.of1(r));
				return true;
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkSaveOutput.this.enqueue(conv.apply((Sdream<V0>) s));
			}
		};
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<Rmap>> conv, int parallelism) {
		return priors(conv);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V0> Output<V0> priorFlat(Function<V0, Sdream<Rmap>> conv) {
		return (Output<V0>) new SparkSaveOutput(spark, targetUri, table()) {
			private static final long serialVersionUID = -2887496205884721038L;

			@Override
			public boolean enqueue(Rmap r) {
				if (null == r) return false;
				SparkSaveOutput.this.enqueue(conv.apply((V0) r));
				return true;
			}

			@Override
			public void enqueue(Sdream<Rmap> s) {
				SparkSaveOutput.this.enqueue(s.mapFlat(r -> conv.apply((V0) r)));
			}
		};
	}

}
