package net.butfly.albatis.spark.io;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
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
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.util.DSdream.$utils$;

public abstract class SparkInputBase<V> extends SparkIO implements OddInput<V> {
	private static final long serialVersionUID = 6966901980613011951L;
	private static final Logger logger = Logger.getLogger(SparkInputBase.class);

	protected Dataset<V> dataset;
	private transient StreamingQuery streaming = null;

	protected SparkInputBase(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		open();
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

	public final Dataset<V> dataset() {
		return dataset;
	}

	<E> void start(Consumer<V> using) {
		if (dataset.isStreaming()) {
			ForeachWriter<V> w = new ForeachWriter<V>() {
				private static final long serialVersionUID = 3602739322755312373L;

				@Override
				public void process(V r) {
					using.accept(r);
				}

				@Override
				public boolean open(long partitionId, long version) {
					return true;
				}

				@Override
				public void close(Throwable err) {}
			};
			DataStreamWriter<V> s = dataset.writeStream();
			s = s.foreach(w);
			s = s.outputMode(OutputMode.Update());
			s = s.trigger(Trigger.ProcessingTime(500));
			streaming = s.start();
		} else dataset.foreach(using::accept);
	}

	void await() {
		if (null != streaming) try {
			streaming.awaitTermination();
		} catch (StreamingQueryException e) {
			logger.error("Stream await fail", e);
		}
	}

	// ---------------------------------------------------------------------
	private static class SparkInputWrapper<VV> extends SparkInputBase<VV> implements SparkIOLess {
		private static final long serialVersionUID = 5957738224117308018L;

		protected SparkInputWrapper(SparkInputBase<?> s, Dataset<VV> ds) {
			super(s.spark, s.targetUri);
			dataset = ds;
		}

		@Override
		protected Dataset<VV> load() {
			return dataset;
		}

		@Override
		protected Map<String, String> options() {
			return null;
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
		return thenFlat(v -> conv.apply(Sdream.of(v)));
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
	@Deprecated
	public final V dequeue() {
		AtomicReference<V> r = new AtomicReference<>();
		deq(r::lazySet);
		return r.get();
	}

	@Override
	@Deprecated
	public final void dequeue(Consumer<Sdream<V>> using) {
		deq(v -> using.accept(Sdream.of(v)));
	}

	@Override
	public void deq(Consumer<V> using) {
		start(using::accept);
	}
}
