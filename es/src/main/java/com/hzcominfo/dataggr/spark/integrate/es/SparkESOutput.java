package com.hzcominfo.dataggr.spark.integrate.es;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;
import com.hzcominfo.dataggr.spark.io.SparkOutput;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;

@Schema({ "es", "elasticsearch" })
public class SparkESOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = 2840201452393061853L;

	protected SparkESOutput(SparkSession spark, URISpec targetUri, String[] table) {
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
