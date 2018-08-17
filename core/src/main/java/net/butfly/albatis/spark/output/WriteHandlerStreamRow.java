package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Output;

class WriteHandlerStreamRow extends WriteHandlerBase<WriteHandlerStreamRow, Row> {
	private DataStreamWriter<Row> w;

	protected WriteHandlerStreamRow(Dataset<Row> ds) {
		super(null);
	}

	@Override
	public void close() {
		StreamingQuery s = w.start();
		try {
			s.awaitTermination();
		} catch (StreamingQueryException e) {
			throw new RuntimeException(e);
		}
	}

	private DataStreamWriter<Row> w() {
		return ds.writeStream().outputMode(OutputMode.Update()).trigger(trigger());
	}

	@Override
	public void save(String format, Map<String, String> options) { // TODO: need two mode
		options.putIfAbsent("checkpointLocation", checkpoint());
		w = w().format(format).options(options);
	}

	@Override
	public void save(Output<Row> output) {
		Map<String, String> opts = Maps.of("checkpointLocation", checkpoint(), "output", output.ser());
		w = w().format(SparkSinkOutput.FORMAT).options(opts);
	}

	protected Trigger trigger() {
		return Trigger.ProcessingTime(0);
	}

}
