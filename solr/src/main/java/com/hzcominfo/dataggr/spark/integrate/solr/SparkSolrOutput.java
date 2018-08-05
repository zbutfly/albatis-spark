package com.hzcominfo.dataggr.spark.integrate.solr;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;

import com.hzcominfo.dataggr.spark.io.SparkOutput;
import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;

@Schema("solr")
public class SparkSolrOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = 1598463842099800246L;

	protected SparkSolrOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	@Override
	public boolean enqueue(Rmap row) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected Map<String, String> options() {
		// TODO Auto-generated method stub
		return null;
	}
}
