package net.butfly.albatis.spark.io;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.pump.Pump;

class SparkPump<V> extends Namedly implements Pump<V>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInputBase<V> input;
	private final Output<V> output;

	SparkPump(SparkInputBase<V> input, Output<V> dest) {
		super(input.name() + ">" + dest.name());
		this.input = input;
		this.output = dest;
		Reflections.noneNull("Pump source/destination should not be null", input, dest);
	}

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();

		/**
		 * Foreach writing (streaming by sink or stocking)<br>
		 * Sink writing by <code>OutputSink</code>, <code>uri</code> in <code>options()</code> is required.
		 */
		Dataset<V> ds = input.dataset;
		if (ds.isStreaming()) {
			logger().info("Dataset sink writing: " + ds.toString());
			input.sink(ds, output);
		} else {
			logger().info("Dataset foreach writing: " + ds.toString());
			if (output instanceof OddOutput) ds.foreach(((OddOutput<V>) output)::enqueue);
			else ds.foreach(r -> output.enqueue(Sdream.of1(r)));
		}

		boolean b = true;
		while (b && opened())
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				b = false;
			}
	}

	@Override
	public void close() {
		input.close();
		Pump.super.close();
		output.close();
	}
}
