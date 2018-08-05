package net.butfly.albatis.spark.plugin;

import org.apache.spark.sql.Row;

import net.butfly.albatis.spark.io.SparkInputBase;

public class SparkCommonPluginInput extends SparkPluginInput {
	private static final long serialVersionUID = 2836878221866891514L;

	public SparkCommonPluginInput(SparkInputBase<Row> input, PluginConfig pc) {
		super(input, pc);
	}
}
