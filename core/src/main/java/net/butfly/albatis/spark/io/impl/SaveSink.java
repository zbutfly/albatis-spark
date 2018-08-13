package net.butfly.albatis.spark.io.impl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.util.LongAccumulator;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.$utils$;
import scala.collection.Seq;

public class SaveSink implements Sink, Serializable {
	private static final long serialVersionUID = -4204798653294379200L;
	private static final Logger logger = Logger.getLogger(SaveSink.class);
	private final LongAccumulator acc;
	private final LongAccumulator time;

	private final Map<String, String> options;
	private final String format;

	protected SaveSink(String format, Map<String, String> opts, LongAccumulator acc, LongAccumulator time) {
		this.acc = acc;
		this.time = time;
		this.format = format;
		this.options = opts;
	}

	@Override
	public final void addBatch(long batchId, Dataset<Row> batch) {
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
				.map(r -> OutputSink.rawToRmap(r, batchId, c.incrementAndGet()), $utils$.ENC_R);
		ds.write().options(options).format(format).save();
		long tt = System.currentTimeMillis() - t;
		time.add(tt);
		logger.debug("Sink[" + batchId + "] finished in: " + tt + " ms, avg: " + acc.value() / (time.value() / 1000.0) + " input/s.");
	}

	public static final class SaveSinkProvider implements DataSourceV2, StreamSinkProvider, DataSourceRegister {
		@Override
		public String shortName() {
			return "sink";
		}

		@Override
		public Sink createSink(SQLContext ctx, scala.collection.immutable.Map<String, String> options, Seq<String> partitionColumns,
				OutputMode outputMode) {
			Map<String, String> opts = $utils$.mapizeJava(options);
			String format = opts.remove("format");
			if (null == format) throw new IllegalArgumentException("SaveSink need options include format.");
			return new SaveSink(format, opts, ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":COUNT"), //
					ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":TIME"));
		}
	}
}
