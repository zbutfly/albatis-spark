package net.butfly.albatis.spark.io;

import java.io.Serializable;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.io.impl.WriteHandler;

class SparkPump extends Namedly implements Pump<Rmap>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInputBase<Rmap> input;
	private final Output<Rmap> output;

	SparkPump(SparkInputBase<Rmap> input, Output<Rmap> dest) {
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

		WriteHandler.save(input.dataset, output);

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
