package net.butfly.albatis.spark.plugin;

import static org.apache.spark.sql.functions.col;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.Sparks;

public class SparkCollisionPluginInput extends SparkPluginInput {
	private static final long serialVersionUID = -8270271411455957886L;
	private final Map<SparkInput<Row>, String> cInputs;

	public SparkCollisionPluginInput(SparkInput<Row> input, PluginConfig pc) {
		super(input, pc);
		Map<SparkInput<Row>, String> cInputs = pc.getCollisionInputs();
		if (cInputs == null || cInputs.isEmpty()) throw new RuntimeException("Not conforming to the conditions of collision plugin");
		this.cInputs = cInputs;
	}

	@Override
	public void open() {
		for (SparkInput<Row> in : cInputs.keySet())
			in.open();
		super.open();
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = super.load();
		for (SparkInput<Row> in : cInputs.keySet())
			ds0 = ds0.join(Sparks.union(in.rows().values().iterator()), ds0.col(PLUGIN_KEY).equalTo(col(cInputs.get(in))), "inner");
		return ds0;
	}

	@Override
	public void close() {
		super.close();
		for (SparkInput<Row> in : cInputs.keySet())
			in.close();
	}
}
