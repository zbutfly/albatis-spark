package net.butfly.albatis.spark.io;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Wrapper;
import net.butfly.albatis.spark.io.impl.OutputSink;

public abstract class SparkInputBase<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	protected Dataset<V> dataset;

	protected SparkInputBase(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		open();
	}

	public Dataset<V> dataset() {
		return dataset;
	}

	public String format() {
		return null;
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
	public void open() {
		OddInput.super.open();
		dataset = load();
	}

	protected abstract Dataset<V> load();

	@Override
	public void close() {
		OddInput.super.close();
	}

	// ---------------------------------------------------------------------
	private static class SparkInputWrapper<VV> extends SparkInputBase<VV> implements SparkIOLess, Wrapper<SparkInputBase<VV>> {
		private static final long serialVersionUID = 5957738224117308018L;
		private final SparkInputBase<?> base;

		protected SparkInputWrapper(SparkInputBase<?> s, Dataset<VV> ds) {
			super(s.spark, s.targetUri);
			this.base = s;
			dataset = ds;
		}

		@Override
		protected Dataset<VV> load() {
			return dataset;
		}

		@Override
		protected Map<String, String> options() {
			return base.options();
		}

		@Override
		protected <T> void sink(Dataset<T> ds, Output<?> output) {
			base.sink(ds, output);
		}

		@Override
		public <BB extends IO> BB bases() {
			return Wrapper.bases(base);
		}
	}

	@Override
	public SparkInputBase<V> filter(Predicate<V> predicater) {
		return new SparkInputWrapper<>(this, dataset.filter(predicater::test));
	}

	@Override
	@Deprecated
	public SparkInputBase<V> filter(Map<String, Object> criteria) {
		throw new UnsupportedOperationException();
	}

	/*
	 * destination class should be Rmap or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> SparkInputBase<V1> then(Function<V, V1> conv) {
		return new SparkInputWrapper<>(this, (Dataset<V1>) dataset.map(r -> (Rmap) conv.apply(r), $utils$.ENC_R));
	}

	@Override
	public <V1> SparkInputBase<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return thenFlat(v -> conv.apply(Sdream.of1(v)));
	}

	@Override
	@SuppressWarnings("deprecation")
	public <V1> SparkInputBase<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return thens(conv);
	}

	/*
	 * destination class should be Rmap or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> SparkInputBase<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		return new SparkInputWrapper<>(this, (Dataset<V1>) dataset.flatMap(v -> ((List<Rmap>) conv.apply(v).list()).iterator(),
				$utils$.ENC_R));
	}

	@Override
	public SparkPump<V> pump(int parallelism, Output<V> dest) {
		return new SparkPump<V>(this, dest);
	}

	public SparkPump<V> pump(Output<V> output) {
		return pump(-1, output);
	}

	@Override
	public int features() {
		int f = super.features();
		if (dataset.isStreaming()) f |= IO.Feature.STREAMING;
		return f;
	}

	protected Trigger trigger() {
		return Trigger.ProcessingTime(0);
	}

	protected <T> void sink(Dataset<T> ds, Output<?> output) {
		DataStreamWriter<T> ss = ds.writeStream().outputMode(OutputMode.Update()).trigger(Trigger.ProcessingTime(500))//
				.format(OutputSink.FORMAT).trigger(trigger())//
				.options(Maps.of("checkpointLocation", "/tmp/" + ds.sparkSession().sparkContext().appName(), "output", output.ser()));
		StreamingQuery s = ss.start();
		try {
			s.awaitTermination();
		} catch (StreamingQueryException e) {
			throw new RuntimeException(e);
		}
	}
}
