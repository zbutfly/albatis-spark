package com.hzcominfo.dataggr.spark.join;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkJoinInput extends SparkInput {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final List<SparkInput> inputs;
	protected final String joinType;

	public SparkJoinInput(List<SparkInput> inputs) {
		this("inner", inputs);
	}
	
	public SparkJoinInput(String joinType, List<SparkInput> inputs) {
		if (inputs == null || inputs.size() < 2)
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
		SparkInput in0 = inputs.get(0);
		Dataset<Row> ds0 = in0.dataset();
		for (int i = 1; i < inputs.size(); i++) {
			SparkInput ini = inputs.get(i);
			Dataset<Row> dsi = ini.dataset();
			//TODO 
//			ds0 = ds0.join(dsi, ds0.col(in0.key()).equalTo(dsi.col(ini.key())), joinType).distinct();
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
	
	@Override
	public void close() {
		super.close();
		for (SparkInput in : inputs)
			in.close();
	}
}
