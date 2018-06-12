package com.hzcominfo.dataggr.spark.plugin;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

public class SparkCollisionPluginInput extends SparkPluginInput {
	private static final long serialVersionUID = -8270271411455957886L;
	private final Map<String, SparkInput> cInputs;

	public SparkCollisionPluginInput(SparkInput input, PluginConfig pc) {
		super(input, pc);
		Map<String, SparkInput> cInputs = pc.getCollisionInputs();
		if (cInputs == null || cInputs.isEmpty())
			throw new RuntimeException("Not conforming to the conditions of collision plugin");
		this.cInputs = cInputs;
	}

	@Override
	public void open() {
		super.open();
		for (SparkInput in : cInputs.values())
			in.open();
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds0 = super.load();
		for (String key : cInputs.keySet()) {
			SparkInput in = cInputs.get(key);
			Dataset<Row> ds = in.dataset();
			ds0 = ds0.join(ds, ds0.col(PLUGIN_KEY).equalTo(ds.col(key)), "inner").distinct();
		}
		return ds0;
	}

	@Override
	protected Map<String, String> options() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected String schema() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		super.close();
		for (SparkInput in : cInputs.values())
			in.close();
	}
}
