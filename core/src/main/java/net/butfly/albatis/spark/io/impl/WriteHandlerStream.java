package net.butfly.albatis.spark.io.impl;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkSaveOutput;

class WriteHandlerStream extends WriteHandlerBase<DataStreamWriter<Rmap>, WriteHandlerStream> {
	protected WriteHandlerStream(Dataset<Rmap> ds, Output<Rmap> output) {
		super(ds, output);
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

	@Override
	protected void save() {
		w = ds.writeStream().outputMode(OutputMode.Update()).trigger(trigger());
		if (output instanceof SparkSaveOutput) {
			SparkSaveOutput o = (SparkSaveOutput) output;
			output.logger().info("Dataset [" + ds.toString() + "] streaming save to: " + o.format());
			Map<String, String> opts = o.options();
			opts.putIfAbsent("checkpointLocation", checkpoint());
			w = w.format(o.format()).options(opts);
		} else {
			output.logger().info("Dataset [" + ds.toString() + "] streaming sink to: " + output.name());
			Map<String, String> opts = Maps.of("checkpointLocation", checkpoint(), "output", output.ser());
			w = w.format(OutputSink.FORMAT).options(opts);
		}
	}
}
