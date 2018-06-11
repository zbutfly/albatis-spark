package com.hzcominfo.dataggr.spark.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput[] inputs) {
		super("inner", inputs);
	}

	@Override
	protected Dataset<Row> load() {
		SparkInput in0 = inputs[0];
		Dataset<Row> ds0 = in0.dataset();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (int i = 1; i < inputs.length; i++) {
			SparkInput ini = inputs[i];
			Dataset<Row> dsi = ini.dataset();
			dsAll.add(ds0.join(dsi, ds0.col(in0.key()).equalTo(dsi.col(ini.key())), joinType).distinct());
		}
		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0;
	}
}
