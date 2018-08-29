package net.butfly.albatis.spark.output;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.util.LongAccumulator;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkOutput;
import net.butfly.albatis.spark.impl.Sparks;
import scala.collection.Seq;
import static net.butfly.albatis.spark.impl.Schemas.compute;

/**
 * Streaming sink writing
 */
public abstract class SparkSinkOutputBase extends SparkOutput<Rmap> {
	private static final long serialVersionUID = -1L;

	public SparkSinkOutputBase(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public abstract void save(Dataset<Row> ds);

	@Override
	public String format() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, String> options(String table) {
		return Maps.of();
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
			logger.debug("Sink [" + batchId + ", streaming: " + batch.isStreaming() + "] started.");
			long start = System.currentTimeMillis();

			// XXX: Fxxk spark streaming, the streaming dataset in batch can and can only be collected!
			List<Row> rows = batch.collectAsList();
			acc.add(rows.size());
			logger.trace("Sink [" + batchId + ", streaming: " + batch.isStreaming() + "] collected: "//
					+ rows.size() + ", total: " + acc.value());
			if (rows.isEmpty()) return;
			Dataset<Row> ds = batch.sparkSession().createDataset(rows, RowEncoder.apply(batch.schema()));
			// XXX: now the batch is frame, not streaming.
			compute(ds).forEach(t -> {
				try (WriteHandler w = WriteHandler.of(t._2);) {
					w.save(t._1, output);
				}
			});
			long spent = System.currentTimeMillis() - start;
			time.add(spent);
			logger.debug("Sink[" + batchId + "] finished in: " + spent + " ms, "//
					+ "avg: " + acc.value() / (time.value() / 1000.0) + " input/s.");
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
			Map<String, String> opts = Sparks.mapizeJava(options);
			String code = opts.get("output");
			Output<Rmap> o = IO.der(code);
			return new OutputSink(o, ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":COUNT"), //
					ctx.sparkContext().longAccumulator(ctx.sparkContext().appName() + ":TIME"));
		}
	}
}
