package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddOutput;

public abstract class SparkOutput extends SparkIO implements OddOutput<Row>, Serializable {
	private static final long serialVersionUID = 7339834746933783020L;

	public SparkOutput() {}

	protected SparkOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}
}
