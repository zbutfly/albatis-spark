package com.hzcominfo.dataggr.spark.plugin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkInput;

/**
 * for feature
 * 
 * @author chenw
 *
 */
public class SparkPluginInput extends SparkInput {
	private static final long serialVersionUID = -3514105763334222049L;
	protected final SparkInput input;
	protected final PluginConfig pc;
	protected final static String PLUGIN_KEY = PluginElement.zjhm.name();
	protected final static String COUNT = PluginElement.count.name();
	protected final static String MAX_SCORE = PluginElement.max_score.name();

	public SparkPluginInput(SparkInput input, PluginConfig pc) {
		this.input = input;
		this.pc = pc;
	}

	@Override
	public void open() {
		input.open();
		super.open();
	}

	@Override
	protected Dataset<Row> load() {
		Dataset<Row> ds = input.dataset();
		List<String> keys = pc.getKeys();
		String maxScore = pc.getMaxScore();
		List<Dataset<Row>> dsList = new ArrayList<>();
		keys.forEach(k -> dsList
				.add(ds.groupBy(col(k).as(PLUGIN_KEY)).agg(count("*").as(COUNT), max(maxScore).as(MAX_SCORE))));
		// if (dsList.isEmpty()) return null;
		Dataset<Row> ds0 = dsList.get(0);
		for (int i = 1; i < dsList.size(); i++)
			ds0.union(dsList.get(i));
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
		input.close();
	}
}
