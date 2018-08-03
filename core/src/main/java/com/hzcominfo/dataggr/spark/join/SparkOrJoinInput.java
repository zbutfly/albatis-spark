package com.hzcominfo.dataggr.spark.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albatis.io.R;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected Dataset<R> load() {
		Dataset<Row> ds0 = input.dataset();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (SparkInput<?> in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<?> ds = in.dataset();
			dsAll.add(ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct());
		}

		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0.map(FuncUtil::rMap, FuncUtil.ENC_R);
	}
}
