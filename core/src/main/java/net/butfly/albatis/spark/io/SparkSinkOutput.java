package net.butfly.albatis.spark.io;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public abstract class SparkSinkOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = 6425084834245827604L;

	protected SparkSinkOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public abstract String format();

	@Override
	public Map<String, String> options() {
		return Maps.of("format", format());
	}

	@Override
	public final boolean enqueue(Rmap v) {
		throw new UnsupportedOperationException();
	}

	// =====
	@SuppressWarnings("unchecked")
	@Override
	public <V0> Output<V0> prior(Function<V0, Rmap> conv) {
		return (Output<V0>) new SparkSinkOutput(spark, targetUri) {
			private static final long serialVersionUID = 149005111794464144L;

			@Override
			public String format() {
				return SparkSinkOutput.this.format();
			}

			@Override
			public Map<String, String> options() {
				return SparkSinkOutput.this.options();
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<Rmap>> conv) {
		return (Output<V0>) new SparkSinkOutput(spark, targetUri) {
			private static final long serialVersionUID = 6402682488771912519L;

			@Override
			public String format() {
				return SparkSinkOutput.this.format();
			}

			@Override
			public Map<String, String> options() {
				return SparkSinkOutput.this.options();
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
		return (Output<V0>) new SparkSinkOutput(spark, targetUri) {
			private static final long serialVersionUID = -7308430583750627337L;

			@Override
			public String format() {
				return SparkSinkOutput.this.format();
			}

			@Override
			public Map<String, String> options() {
				return SparkSinkOutput.this.options();
			}
		};
	}
}
