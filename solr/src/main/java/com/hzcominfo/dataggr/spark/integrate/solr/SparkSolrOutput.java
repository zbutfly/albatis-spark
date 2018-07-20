package com.hzcominfo.dataggr.spark.integrate.solr;

import java.util.Map;

import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkOutput;

public class SparkSolrOutput extends SparkOutput {
	private static final long serialVersionUID = 1598463842099800246L;

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
		return "solr,zk";
	}
}
