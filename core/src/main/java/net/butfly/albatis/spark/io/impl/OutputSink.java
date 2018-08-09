package net.butfly.albatis.spark.io.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.streaming.OutputMode;

import com.hzcominfo.albatis.nosql.Connection.DriverManager;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.$utils$;
import scala.collection.Seq;

public class OutputSink implements Sink {
	private static final Logger logger = Logger.getLogger(OutputSink.class);
	public static final String FORMAT = OutputSinkProvider.class.getName();
	private final URISpec targetUri;
	protected final transient Output<Rmap> output;

	public OutputSink(String uri) {
		targetUri = new URISpec(uri);
		try {
			output = DriverManager.connect(targetUri).output();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		logger.info("Native output [" + output.getClass().getName() + "] constructed on worker...HEAVILY!!");
	}

	@Override
	public void addBatch(long batchId, Dataset<Row> batch) {
		long t = System.currentTimeMillis();
		List<Row> rows = batch.collectAsList();
		logger.debug("Sink[" + batchId + ", streaming: " + batch.isStreaming() + "]: " + rows.size());
		Dataset<Row> ds;
		try (JavaSparkContext jsc = new JavaSparkContext(batch.sparkSession().sparkContext());) {
			ds = batch.sparkSession().createDataFrame(jsc.parallelize(rows), batch.schema());
		}
		ds.map(row -> {
			byte[] data = row.getAs("value");
			try (ObjectInputStream oss = new ObjectInputStream(new ByteArrayInputStream(data));) {
				return (Rmap) oss.readObject();
			} catch (ClassNotFoundException | IOException e) {
				logger.error("Sinked row data [" + data.length + "] corrupted.");
				throw new RuntimeException(e);
			}
		}, $utils$.ENC_R).foreachPartition(itor -> output.enqueue(Sdream.of(() -> itor)));
		logger.debug("Sink[" + batchId + "] finished in: " + (System.currentTimeMillis() - t) + " ms.");
	}

	public static class OutputSinkProvider implements DataSourceV2, StreamSinkProvider, DataSourceRegister {
		@Override
		public String shortName() {
			return "output";
		}

		@Override
		public Sink createSink(SQLContext ctx, scala.collection.immutable.Map<String, String> options, Seq<String> partitionColumns,
				OutputMode outputMode) {
			return new OutputSink($utils$.mapizeJava(options).get("uri"));
		}
	}
}
