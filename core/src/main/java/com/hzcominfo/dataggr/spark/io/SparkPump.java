package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import org.apache.spark.sql.Row;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.pump.Pump;

class SparkPump extends Namedly implements Pump<Row>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInput input;
	private final OddOutput<Row> output;

	SparkPump(SparkInput input, OddOutput<Row> output) {
		super(input.name() + ">" + output.name());
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);

	}

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();
		input.start(output);
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
