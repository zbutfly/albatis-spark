package net.butfly.albatis.spark.plugin;

import static net.butfly.albatis.spark.impl.Sparks.alias;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
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
	protected List<Dataset<Row>> load() {
		List<Dataset<Row>> dss = super.load();
		List<Dataset<Row>> dss1 = Colls.list();
		cInputs.forEach((in, key) -> dss.forEach(ds -> dss1.add(//
				ds.join(Sparks.union(in.rows()), ds.col(PLUGIN_KEY).equalTo(col(key)), "inner").alias(alias(ds)))));
		return dss;
	}

	@Override
	public void close() {
		super.close();
		for (SparkInput<Row> in : cInputs.keySet())
			in.close();
	}
}
