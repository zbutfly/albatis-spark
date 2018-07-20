package com.hzcominfo.dataggr.spark.integrate.es;

import java.util.Map;

import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

public class SparkESOutput extends SparkOutput {
	private static final long serialVersionUID = 2840201452393061853L;

	@Override
	public boolean enqueue(Row row) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected Map<String, String> options() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String schema() {
		return "es,elasticsearch";
	}
}
