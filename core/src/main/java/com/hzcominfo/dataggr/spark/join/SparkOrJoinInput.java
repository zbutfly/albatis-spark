package com.hzcominfo.dataggr.spark.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;
	
	public SparkOrJoinInput(SparkInput input, String col, Map<String, SparkInput> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = input.dataset();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (String key : joinInputs.keySet()) {
			SparkInput in = joinInputs.get(key);
			Dataset<Row> ds = in.dataset();
			dsAll.add(ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct());
		}
		
		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0;
	}
}
