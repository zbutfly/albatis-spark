package com.hzcominfo.dataggr.spark.plugin;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInputBase;

public class SparkCollisionPluginInput extends SparkPluginInput {
	private static final long serialVersionUID = -8270271411455957886L;
	private final Map<SparkInputBase<Row>, String> cInputs;

	public SparkCollisionPluginInput(SparkInputBase<Row> input, PluginConfig pc) {
		super(input, pc);
		Map<SparkInputBase<Row>, String> cInputs = pc.getCollisionInputs();
		if (cInputs == null || cInputs.isEmpty()) throw new RuntimeException("Not conforming to the conditions of collision plugin");
		this.cInputs = cInputs;
	}

	@Override
	public void open() {
		for (SparkInputBase<Row> in : cInputs.keySet())
			in.open();
		super.open();
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = super.load();
		for (SparkInputBase<Row> in : cInputs.keySet()) {
			String key = cInputs.get(in);
			Dataset<Row> ds = in.dataset();
			ds0 = ds0.join(ds, ds0.col(PLUGIN_KEY).equalTo(ds.col(key)), "inner");
		}
		return ds0;
	}

	@Override
	protected Map<String, String> options() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		super.close();
		for (SparkInputBase<Row> in : cInputs.keySet())
			in.close();
	}
}
