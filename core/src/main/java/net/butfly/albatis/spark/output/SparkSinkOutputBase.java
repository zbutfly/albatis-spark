package net.butfly.albatis.spark.output;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.util.LongAccumulator;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkOutput;
import net.butfly.albatis.spark.impl.Sparks;
import scala.collection.Seq;

/**
 * Streaming sink writing
 */
public abstract class SparkSinkOutputBase extends SparkOutput<Rmap> {
	private static final long serialVersionUID = -1L;

	protected SparkSinkOutputBase(SparkSession spark) {
		super(spark, null);
	}

	public SparkSinkOutputBase(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public void enqueue(Sdream<Rmap> r) {
		save(dsd(r).ds);
	}

	@Override
	public abstract void save(Dataset<Rmap> ds);

	@Override
	public String format() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, String> options() {
		throw new UnsupportedOperationException();
	}

	public static class OutputSink implements Sink, Serializable {
		private static final long serialVersionUID = -2583630312009265765L;
		private static final Logger logger = Logger.getLogger(OutputSink.class);
		protected final Output<Rmap> output;
		private final LongAccumulator acc;
		private final LongAccumulator time;

		public OutputSink(Output<Rmap> output, LongAccumulator acc, LongAccumulator time) {
			this.output = output;
			this.acc = acc;
			this.time = time;
		}

		@Override
		public void addBatch(long batchId, Dataset<Row> batch) {
			SparkContext sc = batch.sparkSession().sparkContext();
			logger.debug("Sink [" + batchId + ", streaming: " + batch.isStreaming() + "] started.");
			long t = System.currentTimeMillis();
			List<Row> rows = batch.collectAsList();
			acc.add(rows.size());
			logger.trace("Sink [" + batchId + ", streaming: " + batch.isStreaming() + "] collected: "//
					+ rows.size() + ", total: " + acc.value());
			if (rows.isEmpty()) return;
			AtomicLong c = new AtomicLong();
			@SuppressWarnings("resource")
			Dataset<Rmap> ds = batch.sparkSession().createDataFrame(new JavaSparkContext(sc).parallelize(rows), batch.schema())//
					.map(r -> rawToRmap(r, batchId, c.incrementAndGet()), Sparks.ENC_R);
			try (WriteHandler<Rmap> w = WriteHandler.of(ds);) {
				w.save(output);
			}
			long tt = System.currentTimeMillis() - t;
			time.add(tt);
			logger.debug("Sink[" + batchId + "] finished in: " + tt + " ms, avg: " + acc.value() / (time.value() / 1000.0) + " input/s.");
		}

		public static Rmap rawToRmap(Row row, long batchId, long num) {
			byte[] data = row.getAs("value");
			Rmap r = null;
			try (ObjectInputStream oss = new ObjectInputStream(new ByteArrayInputStream(data));) {
				return (r = (Rmap) oss.readObject());
			} catch (ClassNotFoundException | IOException e) {
				logger.error("Sinked row data [" + data.length + "] corrupted.", e);
				throw new RuntimeException(e);
			} finally {
				if (null != r && num % 30000 == 0) logger.trace("[" + Thread.currentThread().getName() + //
						"][" + num + "]\n\tRmap<=== " + r.toString() + "\n\t Row<=== " + Sparks.debug(row));
			}
		}
	}

	public static class OutputSinkProvider implements DataSourceV2, StreamSinkProvider, DataSourceRegister {
		@Override
		public String shortName() {
			return "output";
		}

		@Override
		public Sink createSink(SQLContext ctx, scala.collection.immutable.Map<String, String> options, Seq<String> partitionColumns,
				OutputMode outputMode) {
			String code = Sparks.mapizeJava(options).get("output");
			Output<Rmap> o = IO.der(code);
			return new OutputSink(o, ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":COUNT"), //
					ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":TIME"));
		}
	}
}
