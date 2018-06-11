package com.hzcominfo.dataggr.spark.io;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkJoinInput extends SparkInput {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInput[] inputs;
	protected final String joinType;

	public SparkJoinInput(SparkInput... inputs) {
		this("inner", inputs);
	}
	
	public SparkJoinInput(String joinType, SparkInput... inputs) {
		if (inputs == null || inputs.length < 2)
			throw new RuntimeException("Not conforming to the conditions of join");
		this.joinType = joinType;
		this.inputs = inputs;
	}
	
	@Override
	public void open() {
		for (SparkInput in : inputs)
			in.open();
		super.open();
	}

	@Override
	protected Dataset<Row> load() {
		SparkInput in0 = inputs[0];
		Dataset<Row> ds0 = in0.dataset();
		for (int i = 1; i < inputs.length; i++) {
			SparkInput ini = inputs[i];
			Dataset<Row> dsi = ini.dataset();
			ds0 = ds0.join(dsi, ds0.col(in0.key()).equalTo(dsi.col(ini.key())), joinType).distinct();
		}
		return ds0;
	}

	@Override
	protected Map<String, String> options() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected String schema() {
		throw new UnsupportedOperationException();
	}
}
