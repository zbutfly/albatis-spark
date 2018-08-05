package net.butfly.albatis.spark.io;

import java.io.Serializable;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Function;
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

	interface PumpFunction<T, RR> extends Function<T, RR>, Serializable {
		static final long serialVersionUID = -5417601923795663049L;
	}

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();

		input.deq(output.odd()::enqueue);
		input.await();

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
