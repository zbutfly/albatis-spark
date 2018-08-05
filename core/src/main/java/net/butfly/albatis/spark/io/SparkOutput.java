package net.butfly.albatis.spark.io;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddOutput;

public abstract class SparkOutput<V> extends SparkIO implements OddOutput<V> {
	private static final long serialVersionUID = 7339834746933783020L;

	protected SparkOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}
}
