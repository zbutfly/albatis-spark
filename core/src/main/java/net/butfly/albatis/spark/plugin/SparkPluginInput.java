package net.butfly.albatis.spark.plugin;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.spark.SparkInput;

public class SparkPluginInput extends SparkInput<Row> {
	private static final long serialVersionUID = -3514105763334222049L;
	protected final SparkInput<Row> input;
	protected final PluginConfig pc;
	protected final static String PLUGIN_KEY = PluginElement.zjhm.name();
	protected final static String COUNT = PluginElement.count.name();
	protected final static String MAX_SCORE = PluginElement.max_score.name();

	public SparkPluginInput(SparkInput<Row> input, PluginConfig pc) {
		super(input.spark, input.targetUri);
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
		String maxScore = pc.getMaxScore();
		List<Dataset<Row>> dsList = new ArrayList<>();
		pc.getKeys().forEach(k -> dsList.add(input.vals().groupBy(col(k).as(PLUGIN_KEY))//
				.agg(count("*").as(COUNT), max(maxScore).as(MAX_SCORE))));
		// if (dsList.isEmpty()) return null;
		Dataset<Row> ds0 = dsList.get(0);
		for (int i = 1; i < dsList.size(); i++)
			ds0.union(dsList.get(i));
		return ds0;
	}

	@Override
	public void close() {
		super.close();
		input.close();
	}
}
