package net.butfly.albatis.spark.join;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIOLess;
import net.butfly.albatis.spark.io.SparkInputBase;
import net.butfly.albatis.spark.util.FuncUtil;

@SuppressWarnings("rawtypes")
public class SparkJoinInput extends SparkInputBase<Rmap> implements SparkIOLess {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInputBase<Row> input;
	protected final String col;
	protected final Map<SparkInputBase<?>, String> joinInputs;
	protected final String joinType;

	public SparkJoinInput(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs, String joinType) {
		super(input.spark, input.targetUri);
		if (joinInputs == null || joinInputs.size() < 1) throw new RuntimeException("Not conforming to the conditions of join");
		this.input = input;
		this.col = col;
		this.joinInputs = joinInputs;
		this.joinType = joinType;
	}

	@Override
	public void open() {
		for (SparkInputBase<?> in : joinInputs.keySet())
			in.open();
		input.open();
		super.open();
	}

	@Override
	protected Dataset<Rmap> load() {
		Dataset<Row> ds0 = input.dataset();
		for (SparkInputBase<?> in : joinInputs.keySet()) {
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
	public void close() {
		super.close();
		input.close();
		for (SparkInputBase in : joinInputs.keySet())
			in.close();
	}
}
