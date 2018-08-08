package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseSink implements Sink {
	private static final Logger logger = Logger.getLogger(HbaseSink.class);
	private final Map<String, String> options = Maps.of();
	private final URISpec targetUri;
	protected final transient HbaseConnection hc;

	public HbaseSink(Map<String, String> options) throws IOException {
		this.options.putAll(options);
		targetUri = new URISpec(this.options.remove("uri"));
		hc = new HbaseConnection(targetUri);
		logger.info("Hbase native connection constructed by worker...HEAVILY!!");
	}

	@Override
	public void addBatch(long batchId, Dataset<Row> batch) {
		logger.debug("Sink[" + batchId + ", streaming: " + batch.isStreaming() + "] begining!");
		if (!batch.isStreaming()) batch.sparkSession().createDataFrame(batch.javaRDD(), batch.schema())//
				.write().options(options).format("org.apache.spark.sql.execution.datasources.hbase").save();
		else {
			List<Row> rows = batch.collectAsList();
			List<String> l = Colls.list();
			for (Row row : rows) {
				byte[] data = row.getAs("value");
				String rmap = Bytes.toString(data);
				logger.trace(rmap);
				l.add(rmap);
			}
			logger.trace(String.join("\n", l));
		}
		logger.debug("Sink[" + batchId + "] finished!");
	}

	public static class BatchWriter extends ForeachWriter<Row> {
		private static final long serialVersionUID = 1713928059037585526L;
		private final Map<String, List<org.apache.hadoop.hbase.client.Row>> rows = Maps.of();
		private final Dataset<Row> batch;
		private final AtomicInteger count = new AtomicInteger(0);

		public static void r(Dataset<Row> batch, HbaseConnection hc) {
			new BatchWriter(batch).run().rows.forEach((t, l) -> {
				try {
					hc.put(t, l);
				} catch (IOException e) {}
			});
		}

		public BatchWriter(Dataset<Row> batch) {
			super();
			this.batch = batch;
		}

		public BatchWriter run() {
			StreamingQuery s = batch.writeStream().foreach(this).outputMode(OutputMode.Update()).start();
			try {
				s.awaitTermination();
			} catch (StreamingQueryException e) {
				logger.error("Streaming in sink batch fail", e);
			}
			logger.debug("Sink by streaming terminated on count: " + count.get() + "!");
			return this;
		}

		private Pair<String, org.apache.hadoop.hbase.client.Row> conv(Row row) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void process(Row row) {
			count.incrementAndGet();
			Pair<String, org.apache.hadoop.hbase.client.Row> p = conv(row);
			if (null != p) rows.computeIfAbsent(p.v1(), t -> Colls.list()).add(p.v2());
		}

		@Override
		public boolean open(long partitionId, long version) {
			return true;
		}

		@Override
		public void close(Throwable err) {
			logger.error("Foreach in sink batch close", err);
		}
	}
}
