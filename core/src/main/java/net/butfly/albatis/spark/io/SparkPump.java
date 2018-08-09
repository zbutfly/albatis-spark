package net.butfly.albatis.spark.io;

import static net.butfly.albatis.spark.io.SparkIO.$utils$.each;
import static net.butfly.albatis.spark.io.SparkIO.$utils$.sink;

import java.io.Serializable;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
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

		if (input.dataset.isStreaming()) sink(input.dataset, output.target());
		else each(input.dataset, output);

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
