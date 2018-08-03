package com.hzcominfo.dataggr.spark.integrate.es;

import java.util.Map;

import net.butfly.albatis.io.R;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

public class SparkESOutput extends SparkOutput<R> {
	private static final long serialVersionUID = 2840201452393061853L;

	@Override
	public boolean enqueue(R row) {
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
