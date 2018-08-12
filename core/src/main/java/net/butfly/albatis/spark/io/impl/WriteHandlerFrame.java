package net.butfly.albatis.spark.io.impl;

import java.util.Map;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkSaveOutput;

class WriteHandlerFrame extends WriteHandlerBase<DataFrameWriter<Rmap>, WriteHandlerFrame> {
	protected WriteHandlerFrame(Dataset<Rmap> ds, Output<Rmap> output) {
		super(ds, output);
	}

	@Override
	protected void save() {
		w = ds.write();
		if (output instanceof SparkSaveOutput) {
			SparkSaveOutput o = (SparkSaveOutput) output;
			output.logger().info("Dataset [" + ds.toString() + "] save to: " + o.format());
			Map<String, String> opts = o.options();
			opts.putIfAbsent("checkpointLocation", checkpoint());
			w.format(o.format()).options(opts).save();
		} else {
			output.logger().info("Dataset [" + ds.toString() + "] foreach to: " + output.name());
			Map<String, String> opts = Maps.of("checkpointLocation", checkpoint(), "output", output.ser());
			w = w.format(OutputSink.FORMAT).options(opts);
			if (output instanceof OddOutput) ds.foreach(((OddOutput<Rmap>) output)::enqueue);
			else ds.foreachPartition(r -> output.enqueue(Sdream.of(r)));
		}
	}
}
