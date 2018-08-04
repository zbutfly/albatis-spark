package com.hzcominfo.dataggr.spark.io;

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

	public SparkInput() {
		super();
	}

	public SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected Dataset<R> load() {
		return spark.readStream().format(format()).options(options()).load().map(this::conv, FuncUtil.ENC_R);
	}

	protected String table() {
		return targetUri.getFile();
	}

	protected R conv(Row row) {
		return new R(table(), FuncUtil.rowMap(row));
	}
}
