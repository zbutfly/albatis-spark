package net.butfly.albatis.spark.io;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.spark.util.DSdream;

public abstract class SparkOutput<V> extends SparkIO implements OddOutput<V> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public URISpec target() {
		return targetUri;
	}

	// @Override
	// public String format() {
	// return OutputSink.FORMAT;
	// }

	protected boolean writing(long partitionId, long version) {
		return true;
	}

	// ============================
	@Override
	public <V0> Output<V0> prior(Function<V0, V> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables) {
			private static final long serialVersionUID = -1680036215116179632L;

			@Override
			public boolean enqueue(V0 r) {
				if (null == r) return false;
				return SparkOutput.this.enqueue(conv.apply((V0) r));
			}

			@Override
			public void enqueue(Sdream<V0> s) {
				SparkOutput.this.enqueue((DSdream<V>) s.map(r -> conv.apply((V0) r)));
			}
		};
	}

	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return new SparkOutput<V0>(spark, targetUri, tables) {
			private static final long serialVersionUID = 5079963400315523098L;

			@Override
			public boolean enqueue(V0 r) {
				if (null == r) return false;
				enqueue(Sdream.of1(r));
				return true;
			}

			@Override
			public void enqueue(Sdream<V0> s) {
				SparkOutput.this.enqueue(conv.apply(s));
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
			public boolean enqueue(V0 r) {
				if (null == r) return false;
				SparkOutput.this.enqueue(conv.apply(r));
				return true;
			}

			@Override
			public void enqueue(Sdream<V0> s) {
				SparkOutput.this.enqueue(s.mapFlat(r -> conv.apply((V0) r)));
			}
		};
	}
}
