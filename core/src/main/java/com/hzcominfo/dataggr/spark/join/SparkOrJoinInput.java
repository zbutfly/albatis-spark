package com.hzcominfo.dataggr.spark.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(List<SparkInput> inputs) {
		super("inner", inputs);
	}

	@Override
	protected Dataset<Row> load() {
		SparkInput in0 = inputs.get(0);
		Dataset<Row> ds0 = in0.dataset();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (int i = 1; i < inputs.size(); i++) {
			SparkInput ini = inputs.get(i);
			Dataset<Row> dsi = ini.dataset();
//			dsAll.add(ds0.join(dsi, ds0.col(in0.key()).equalTo(dsi.col(ini.key())), joinType).distinct());
		}
		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0;
	}
}
