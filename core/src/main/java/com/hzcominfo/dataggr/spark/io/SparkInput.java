package com.hzcominfo.dataggr.spark.io;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.R;

/**
 * generally, any kafka with value of a serialized map should come from here
 */
public abstract class SparkInput extends SparkInputBase<R> {
	private static final long serialVersionUID = 8309576584660953676L;
	protected String[] tables;

	public SparkInput() {
		super();
	}

	public SparkInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Dataset<R> load() {
		Map<String, String> opts = options();
		logger().info("Spark input [" + getClass().toString() + "] constructing: " + opts.toString());
		return spark.readStream().format(format()).options(opts).load().map(this::conv, FuncUtil.ENC_R);
	}

	protected R conv(Row row) {
		return new R(table(), FuncUtil.rowMap(row));
	}
}
