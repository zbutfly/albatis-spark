package com.hzcominfo.dataggr.spark.join;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkJoinInput extends SparkInput {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInput input;
	protected final String col;
	protected final Map<SparkInput, String> joinInputs;
	protected final String joinType;

	public SparkJoinInput(SparkInput input, String col, Map<SparkInput, String> joinInputs, String joinType) {
		if (joinInputs == null || joinInputs.size() < 1)
			throw new RuntimeException("Not conforming to the conditions of join");
		this.input = input;
		this.col = col;
		this.joinInputs = joinInputs;
		this.joinType = joinType;
	}

	@Override
	public void open() {
		for (SparkInput in : joinInputs.keySet())
			in.open();
		input.open();
		super.open();
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = input.dataset();
		for (SparkInput in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<Row> ds = in.dataset();
			ds0 = ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct();
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
		input.close();
		for (SparkInput in : joinInputs.keySet())
			in.close();
	}
}
