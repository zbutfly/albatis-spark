package net.butfly.albatis.spark.io.impl;

import java.io.IOException;
import java.util.List;

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
import net.butfly.albacore.utils.collection.Colls;
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
		List<Row> rows = batch.collectAsList();
		logger.debug("Sink[" + batchId + ", streaming: " + batch.isStreaming() + "]: " + rows.size());
		List<String> l = Colls.list();
		for (Row row : rows) {
			byte[] data = row.getAs("value");
		}
		logger.trace(String.join("\n", l));
		logger.debug("Sink[" + batchId + "] finished!");
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
