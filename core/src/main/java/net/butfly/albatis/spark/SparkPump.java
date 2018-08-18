package net.butfly.albatis.spark;

import java.io.Serializable;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.output.SparkSinkOutput;

class SparkPump extends Namedly implements Pump<Rmap>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInput<Rmap> input;
	private final SparkOutput<Rmap> output;

	SparkPump(SparkInput<Rmap> input, Output<Rmap> dest) {
		super(input.name() + ">" + dest.name());
		this.input = input;
		this.output = dest instanceof SparkOutput ? (SparkOutput<Rmap>) dest : new SparkSinkOutput(input.spark, dest);
		Reflections.noneNull("Pump source/destination should not be null", input, dest);
	}

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();
		output.save(input.dataset);
		close();
	}

	@Override
	public void close() {
		input.close();
		Pump.super.close();
		output.close();
	}
}
