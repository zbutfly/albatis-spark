package net.butfly.albatis.spark.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkInputBase;
import net.butfly.albatis.spark.util.DSdream.$utils$;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected Dataset<Rmap> load() {
		Dataset<Row> ds0 = input.dataset();
		List<Dataset<Row>> dsAll = new ArrayList<>();
		for (SparkInputBase<?> in : joinInputs.keySet()) {
			String key = joinInputs.get(in);
			Dataset<?> ds = in.dataset();
			dsAll.add(ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct());
		}

		for (Dataset<Row> ds : dsAll)
			ds0 = ds0.union(ds);
		return ds0.map(row -> $utils$.rmap(input.table(), row), $utils$.ENC_R);
	}
}
