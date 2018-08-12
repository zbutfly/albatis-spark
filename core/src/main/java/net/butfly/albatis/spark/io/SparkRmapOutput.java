package net.butfly.albatis.spark.io;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;

public abstract class SparkRmapOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = 1509137417654908221L;

	protected SparkRmapOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	protected boolean writing(long partitionId, long version) {
		return true;
	}
}
