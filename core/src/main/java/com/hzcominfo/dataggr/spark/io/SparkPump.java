package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.pump.Pump;

class SparkPump<V> extends Namedly implements Pump<V>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInput input;
	private final OddOutput<V> output;
	private V v;

	SparkPump(SparkInput input, OddOutput<V> output, V v) {
		super(input.name() + ">" + output.name());
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		this.v = v;
	}
	
	interface PumpFunction<T, R> extends Function<T, R>, Serializable {
		static final long serialVersionUID = -5417601923795663049L;
	}

	@SuppressWarnings("unchecked")
	transient PumpFunction<Row, V> converter = r -> {
		if (v == null)
			return (V) r;
		else if (Map.class.isAssignableFrom(v.getClass())) {
			return (V) FuncUtil.rowMap(r);
		} else {
			throw new RuntimeException("There is no transformation method for this class: " + v.getClass().getName());
		}
	};

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();

		input.start(output, converter);
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
