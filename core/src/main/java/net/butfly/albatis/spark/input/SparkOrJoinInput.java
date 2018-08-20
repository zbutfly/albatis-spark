package net.butfly.albatis.spark.input;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.spark.SparkInput;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = input.vals();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (SparkInput<?> in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<?> ds = in.vals();
			dsAll.add(ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct());
		}

		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0;
	}
}
