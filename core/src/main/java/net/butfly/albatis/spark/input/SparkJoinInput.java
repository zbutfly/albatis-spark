package net.butfly.albatis.spark.input;

import static net.butfly.albatis.spark.impl.Sparks.byTable;
import static net.butfly.albatis.spark.impl.Sparks.union;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

@SuppressWarnings("rawtypes")
public class SparkJoinInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInput<Rmap> input;
	protected final String col;
	protected final Map<SparkInput<?>, String> joinInputs;
	protected final String joinType;

	public SparkJoinInput(SparkInput<Rmap> input, String col, Map<SparkInput<?>, String> joinInputs, String joinType) {
		super(input.spark, input.targetUri);
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
	protected List<Dataset<Row>> load() {
		Dataset<Row> ds0 = union(input.rows().iterator());
		for (SparkInput<?> in : joinInputs.keySet())
			for (Dataset<Row> d : in.rows())
				ds0 = ds0.join(d, col(col).equalTo(col(joinInputs.get(in))), joinType).distinct();
		return byTable(ds0);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		for (SparkInput in : joinInputs.keySet())
			in.close();
	}
}
