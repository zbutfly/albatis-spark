package com.hzcominfo.dataggr.spark.join;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkIOLess;
import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albatis.io.R;

@SuppressWarnings("rawtypes")
public class SparkJoinInput extends SparkInput<R> implements SparkIOLess {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInput<Row> input;
	protected final String col;
	protected final Map<SparkInput<?>, String> joinInputs;
	protected final String joinType;

	public SparkJoinInput(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs, String joinType) {
		if (joinInputs == null || joinInputs.size() < 1) throw new RuntimeException("Not conforming to the conditions of join");
		this.input = input;
		this.col = col;
		this.joinInputs = joinInputs;
		this.joinType = joinType;
	}

	@Override
	public void open() {
		for (SparkInput<?> in : joinInputs.keySet())
			in.open();
		input.open();
		super.open();
	}

	@Override
	protected Dataset<R> load() {
		Dataset<Row> ds0 = input.dataset();
		for (SparkInput<?> in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<?> ds = in.dataset();
			ds0 = ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct();
		}
		return ds0.map(FuncUtil::rMap, FuncUtil.ENC_R);
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
