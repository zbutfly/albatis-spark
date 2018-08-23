package net.butfly.albatis.spark.input;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.Sparks;
import static net.butfly.albatis.spark.impl.Sparks.alias;

public class SparkOrJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public SparkOrJoinInput(SparkInput<Rmap> input, String col, Map<SparkInput<?>, String> joinInputs) {
		super(input, col, joinInputs, "inner");
	}

	@Override
	protected List<Dataset<Row>> load() {
		Dataset<Row> ds0 = Sparks.union(input.rows());
		List<Dataset<Row>> dss = Colls.list();
		joinInputs.forEach((in, key) -> in.rows().forEach(ds -> dss.add(//
				ds0.join(ds, ds0.col(col).equalTo(ds.col(key)), joinType).distinct().alias(alias(ds)))));
		return dss;
	}
}
