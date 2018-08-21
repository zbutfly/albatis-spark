package net.butfly.albatis.spark.input;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.Sparks;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput<Rmap> input, String col, Map<SparkInput<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = Sparks.union(input.rows().values().iterator());
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (SparkInput<?> in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<?> ds = Sparks.union(in.rows().values().iterator());
			dsAll.add(ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct());
		}

		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0;
	}
}
