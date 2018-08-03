package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.R;

public abstract class SparkInput<V> extends SparkIO implements Input<V>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private static final Logger logger = Logger.getLogger(SparkInput.class);

	protected transient Dataset<V> dataset;
	private transient StreamingQuery streaming = null;

	public SparkInput() {
		super();
	}

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		open();
	}

	@Override
	public void open() {
		Input.super.open();
		dataset = load();
	}

	protected abstract Dataset<V> load();

	@Override
	public void close() {
		Input.super.close();
	}

	public final Dataset<V> dataset() {
		return dataset;
	}

	<E> void start(Consumer<V> using) {
		if (dataset.isStreaming()) streaming = dataset.writeStream().outputMode(OutputMode.Update()).foreach(new ForeachWriter<V>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(V r) {
				using.accept(r);;
			}

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable err) {}
		}).start();
		else dataset.foreach(using::accept);
	}

	void await() {
		if (null != streaming) try {
			streaming.awaitTermination();
		} catch (StreamingQueryException e) {
			logger.error("Stream await fail", e);
		}
	}

	// ---------------------------------------------------------------------
	private static class SparkInputWrapper<VV> extends SparkInput<VV> implements SparkIOLess {
		private static final long serialVersionUID = 5957738224117308018L;

		protected SparkInputWrapper(SparkInput<?> s, Dataset<VV> ds) {
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

		@Override
		protected String schema() {
			return null;
		}
	}

	@Override
	public void dequeue(Consumer<Sdream<V>> using) {
		start(v -> using.accept(Sdream.of(v)));
	}

	@Override
	public SparkInput<V> filter(Predicate<V> predicater) {
		return new SparkInputWrapper<>(this, dataset.filter(predicater::test));
	}

	@Override
	@Deprecated
	public SparkInput<V> filter(Map<String, Object> criteria) {
		throw new UnsupportedOperationException();
	}

	/*
	 * destination class should be R or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> Input<V1> then(Function<V, V1> conv) {
		return new SparkInputWrapper<>(this, (Dataset<V1>) dataset.map(r -> (R) conv.apply(r), FuncUtil.ENC_R));
	}

	@Override
	public <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv) {
		return thenFlat(v -> conv.apply(Sdream.of(v)));
	}

	@Override
	@SuppressWarnings("deprecation")
	public <V1> Input<V1> thens(Function<Sdream<V>, Sdream<V1>> conv, int parallelism) {
		return thens(conv);
	}

	/*
	 * destination class should be R or Row only....
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <V1> Input<V1> thenFlat(Function<V, Sdream<V1>> conv) {
		return new SparkInputWrapper<>(this, (Dataset<V1>) dataset.flatMap(v -> ((List<R>) conv.apply(v).list()).iterator(),
				FuncUtil.ENC_R));
	}

	@Override
	public SparkPump<V> pump(int parallelism, Output<V> dest) {
		return new SparkPump<V>(this, dest);
	}

	public SparkPump<V> pump(Output<V> output) {
		return pump(-1, output);
	}

	// -------------------------------------
	/**
	 * generally, any kafka with value of a serialized map should come from here
	 */
	public static abstract class SparkRmapInput extends SparkInput<R> {
		private static final long serialVersionUID = 8309576584660953676L;

		public SparkRmapInput() {
			super();
		}

		public SparkRmapInput(SparkSession spark, URISpec targetUri) {
			super(spark, targetUri);
		}

		@Override
		protected Dataset<R> load() {
			return spark.readStream().format(format()).options(options()).load().map(row -> new R(table(), conv(row)), FuncUtil.ENC_R);
		}

		protected String table() {
			return targetUri.getFile();
		}

		protected R conv(Row row) {
			return new R(table(), FuncUtil.rowMap(row));
		}
	}
}
